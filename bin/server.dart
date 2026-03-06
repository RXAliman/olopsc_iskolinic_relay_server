import 'dart:convert';
import 'dart:io';

import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart';
import 'package:shelf_router/shelf_router.dart';
import 'package:shelf_web_socket/shelf_web_socket.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

// ─── In-memory record store ────────────────────────────────────────────────
// Keyed by table name → record ID → record map.
// Records are upserted only if the incoming HLC > stored HLC (string compare
// works because HLC is zero-padded hex: <ts_13>:<counter_4>:<nodeId>).
final Map<String, Map<String, Map<String, dynamic>>> _store = {
  'patients': {},
  'visitations': {},
};

// ─── Connected clients ─────────────────────────────────────────────────────
// Each entry maps a WebSocketChannel sink to metadata about that connection.
class _Client {
  final WebSocketSink sink;

  // Pagination state for sync_request / sync_ack flow.
  List<Map<String, dynamic>>? _pendingPatients;
  List<Map<String, dynamic>>? _pendingVisitations;
  int _cursor = 0;

  _Client(this.sink);
}

final Set<_Client> _clients = {};

// ─── Helpers ───────────────────────────────────────────────────────────────

/// Upsert a record into the in-memory store if its HLC wins.
void _upsert(String table, Map<String, dynamic> record) {
  final id = record['id'] as String?;
  if (id == null) return;

  final existing = _store[table]?[id];
  if (existing != null) {
    final existingHlc = existing['hlc'] as String? ?? '';
    final incomingHlc = record['hlc'] as String? ?? '';
    if (incomingHlc.compareTo(existingHlc) <= 0) return; // existing wins
  }
  _store[table]![id] = record;
}

/// Get all records from both tables with HLC > sinceHlc.
({List<Map<String, dynamic>> patients, List<Map<String, dynamic>> visitations})
_getChangesSince(String sinceHlc) {
  final patients = <Map<String, dynamic>>[];
  final visitations = <Map<String, dynamic>>[];

  for (final entry in _store['patients']!.values) {
    final hlc = entry['hlc'] as String? ?? '';
    if (hlc.compareTo(sinceHlc) > 0) patients.add(entry);
  }
  for (final entry in _store['visitations']!.values) {
    final hlc = entry['hlc'] as String? ?? '';
    if (hlc.compareTo(sinceHlc) > 0) visitations.add(entry);
  }

  return (patients: patients, visitations: visitations);
}

/// Send a JSON message to a client.
void _sendTo(_Client client, Map<String, dynamic> data) {
  try {
    client.sink.add(jsonEncode(data));
  } catch (e) {
    print('Error sending to client: $e');
  }
}

/// Broadcast a message to all clients except the sender.
void _broadcast(Map<String, dynamic> data, {_Client? except}) {
  for (final client in _clients) {
    if (client == except) continue;
    _sendTo(client, data);
  }
}

/// Send the next paginated sync_response batch to a client.
void _sendNextBatch(_Client client, int batchSize) {
  final patients = client._pendingPatients ?? [];
  final visitations = client._pendingVisitations ?? [];

  // Combine into a single list for pagination.
  final combined = <Map<String, dynamic>>[
    for (final p in patients) {'_table': 'patients', ...p},
    for (final v in visitations) {'_table': 'visitations', ...v},
  ];

  final start = client._cursor;
  final end = (start + batchSize).clamp(0, combined.length);
  final batch = start < combined.length ? combined.sublist(start, end) : [];

  // Split batch back into patients and visitations.
  final batchPatients = <Map<String, dynamic>>[];
  final batchVisitations = <Map<String, dynamic>>[];
  for (final item in batch) {
    final table = item['_table'];
    final clean = Map<String, dynamic>.from(item)..remove('_table');
    if (table == 'patients') {
      batchPatients.add(clean);
    } else {
      batchVisitations.add(clean);
    }
  }

  final hasMore = end < combined.length;
  client._cursor = end;

  _sendTo(client, {
    'type': 'sync_response',
    'patients': batchPatients,
    'visitations': batchVisitations,
    'hasMore': hasMore,
  });

  // If no more data, clean up pagination state.
  if (!hasMore) {
    client._pendingPatients = null;
    client._pendingVisitations = null;
    client._cursor = 0;
  }
}

// ─── WebSocket handler ─────────────────────────────────────────────────────

Handler _webSocketHandler() {
  return webSocketHandler((WebSocketChannel channel, String? subprotocol) {
    final client = _Client(channel.sink);
    _clients.add(client);
    print('Client connected. Total clients: ${_clients.length}');

    channel.stream.listen(
      (raw) {
        try {
          final msg = jsonDecode(raw as String) as Map<String, dynamic>;
          final type = msg['type'] as String?;

          switch (type) {
            // ── Heartbeat ──────────────────────────────────────────────
            case 'ping':
              _sendTo(client, {'type': 'pong'});
              break;

            // ── Client pushes local changes ────────────────────────────
            case 'sync_push':
              final table = msg['table'] as String? ?? '';
              final records =
                  (msg['records'] as List?)?.cast<Map<String, dynamic>>() ?? [];

              // Upsert into in-memory store.
              for (final record in records) {
                _upsert(table, record);
              }

              // Broadcast to all OTHER clients (relay).
              _broadcast(msg, except: client);
              break;

            // ── Client requests catch-up sync ──────────────────────────
            case 'sync_request':
              final sinceHlc = msg['sinceHlc'] as String? ?? '';
              final batchSize = msg['batchSize'] as int? ?? 50;

              final changes = _getChangesSince(sinceHlc);
              client._pendingPatients = changes.patients;
              client._pendingVisitations = changes.visitations;
              client._cursor = 0;

              _sendNextBatch(client, batchSize);
              break;

            // ── Client acknowledges batch, wants next ──────────────────
            case 'sync_ack':
              final batchSize = msg['batchSize'] as int? ?? 50;
              _sendNextBatch(client, batchSize);
              break;

            default:
              print('Unknown message type: $type');
          }
        } catch (e) {
          print('Error processing message: $e');
        }
      },
      onDone: () {
        _clients.remove(client);
        print('Client disconnected. Total clients: ${_clients.length}');
      },
      onError: (e) {
        _clients.remove(client);
        print('Client error: $e');
      },
    );
  });
}

// ─── HTTP routes ───────────────────────────────────────────────────────────

final _router = Router()
  ..get('/', _healthHandler)
  ..get('/ws', _webSocketHandler());

Response _healthHandler(Request req) {
  return Response.ok(
    jsonEncode({
      'status': 'ok',
      'server': 'olopsc_iskolinic_relay_server',
      'clients': _clients.length,
      'records': {
        'patients': _store['patients']!.length,
        'visitations': _store['visitations']!.length,
      },
    }),
    headers: {'content-type': 'application/json'},
  );
}

// ─── Entry point ───────────────────────────────────────────────────────────

void main(List<String> args) async {
  final ip = InternetAddress.anyIPv4;

  final handler = Pipeline()
      .addMiddleware(logRequests())
      .addHandler(_router.call);

  final port = int.parse(Platform.environment['PORT'] ?? '8080');
  final server = await serve(handler, ip, port);
  print('Relay server listening on port ${server.port}');
}
