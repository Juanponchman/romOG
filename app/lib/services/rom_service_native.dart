import 'dart:async';
import 'dart:io';
import 'dart:convert';
import 'dart:convert';
import 'dart:io';
import 'dart:io' as io;
import 'dart:isolate';
import 'dart:typed_data';
import 'package:flutter/foundation.dart';
import 'package:http/http.dart' as http;
import 'package:http/io_client.dart';
import 'package:html/parser.dart' as html_parser;
import 'package:romifleur/services/lolroms_scraper.dart';
import 'package:html/dom.dart';
import 'package:romifleur/services/config_service.dart';
import 'package:archive/archive_io.dart';
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';
import 'package:romifleur/models/rom.dart';
import 'package:romifleur/utils/cancellation_token.dart';
import 'package:romifleur/utils/download_exceptions.dart';
import 'package:archive/archive.dart';
import 'package:romifleur/utils/logger.dart';
import 'package:romifleur/services/archive_auth_service.dart';
import 'package:saf_stream/saf_stream.dart';
import 'package:saf_util/saf_util.dart';

const _log = AppLogger('RomService');

// Top-level isolated function for background extraction
// Chunked extraction with intra-file byte-level progress
// Based on v3.3.5 approach with security checks + size verification
Future<void> _isolateExtraction(List<dynamic> args) async {
  final String zipPath = args[0];
  final String destPath = args[1];
  final SendPort sendPort = args[2];

  try {
    final inputStream = InputFileStream(zipPath);
    // Decode ONLY the central directory structure, NOT the content
    final archive = ZipDecoder().decodeBuffer(inputStream);

    // Calculate total uncompressed size for byte-level progress
    int totalBytes = 0;
    for (final file in archive.files) {
      if (file.isFile) totalBytes += file.size;
    }

    int processedBytes = 0;
    int lastUpdateMs = 0;

    sendPort.send(0.0); // Signal: extraction starting

    final outDir = Directory(destPath);
    if (!outDir.existsSync()) {
      outDir.createSync(recursive: true);
    }

    for (final file in archive.files) {
      if (file.isFile) {
        final filePath = p.join(destPath, p.normalize(file.name));

        // Security: reject path traversal
        if (!p.isWithin(p.canonicalize(destPath), p.canonicalize(filePath))) {
          continue;
        }

        // Ensure parent directory exists
        final parentDir = Directory(p.dirname(filePath));
        if (!parentDir.existsSync()) {
          parentDir.createSync(recursive: true);
        }

        final outputStream = OutputFileStream(filePath);

        // STREAMING DECOMPRESSION (Fixes OOM on Android)
        // Access rawContent (compressed stream) instead of content (which triggers full decompress)
        final rawContent = file.rawContent;

        if (rawContent != null) {
          // Convert InputStream to Dart Stream
          final streamController = StreamController<List<int>>();
          // We must read the InputStream in chunks and feed the controller
          // This is synchronous in the isolate but decoupled via stream

          Future<void> feedStream() async {
            try {
              const chunkSize = 1024 * 64; // 64KB chunks
              final length = rawContent.length;
              int readSoFar = 0;

              while (readSoFar < length) {
                final remaining = length - readSoFar;
                final size = remaining < chunkSize ? remaining : chunkSize;
                final bytes = rawContent.readBytes(size).toUint8List();
                streamController.add(bytes);
                readSoFar += size;
                // Yield to event loop to allow processing
                await Future.delayed(Duration.zero);
              }
              await streamController.close();
            } catch (e) {
              streamController.addError(e);
            }
          }

          // Start feeding the stream
          feedStream();

          // Decompress stream using dart:io ZLibDecoder (Deflate)
          // or copy directly if STORE (0)
          Stream<List<int>> decompressedStream;

          if (file.compressionType == ArchiveFile.DEFLATE) {
            // raw: true means raw Deflate (no zlib header), which ZIP uses
            decompressedStream = io.ZLibDecoder(
              raw: true,
            ).bind(streamController.stream);
          } else {
            // STORE (no compression)
            decompressedStream = streamController.stream;
          }

          await for (final chunk in decompressedStream) {
            outputStream.writeBytes(chunk);
            processedBytes += chunk.length;

            final nowMs = DateTime.now().millisecondsSinceEpoch;
            if (nowMs - lastUpdateMs > 100) {
              sendPort.send(processedBytes / totalBytes);
              lastUpdateMs = nowMs;
            }
          }
        } else {
          // Should not happen for valid files
          throw Exception('File content is null');
        }

        outputStream.closeSync();

        // Post-extraction verification
        final extractedFile = File(filePath);
        if (extractedFile.existsSync()) {
          final actualSize = extractedFile.lengthSync();
          if (actualSize != file.size) {
            sendPort.send(
              'Extraction failed: size mismatch for ${file.name} '
              '(expected ${file.size} bytes, got $actualSize bytes)',
            );
            return;
          }
        }
      } else {
        // Directory entry
        final dirPath = p.join(destPath, p.normalize(file.name));
        if (p.isWithin(p.canonicalize(destPath), p.canonicalize(dirPath))) {
          Directory(dirPath).createSync(recursive: true);
        }
      }
    }

    if (totalBytes > 0) sendPort.send(1.0);
    inputStream.closeSync();
    archive.clear();
    sendPort.send(true); // Done
  } catch (e) {
    sendPort.send(e.toString()); // Error
  }
}

class DownloadProgressEvent {
  final double progress; // 0.0 to 1.0 (or > 1.0 for extraction on non-SAF)
  final int receivedBytes;
  final int totalBytes;
  final String?
  phase; // 'download', 'extracting', 'copying' — null = auto-detect via progress

  const DownloadProgressEvent({
    required this.progress,
    required this.receivedBytes,
    required this.totalBytes,
    this.phase,
  });
}

class _CacheEntry {
  final List<RomModel> data;
  final DateTime createdAt;
  _CacheEntry(this.data) : createdAt = DateTime.now();

  // In-memory cache expires after 30 min; disk cache is valid for 7 days.
  bool get isExpired => false;  // Permanent disk cache - never expires
}

class RomService {
  final ConfigService _configService = ConfigService();
  final Map<String, _CacheEntry> _cache = {};
  static const int _maxCacheEntries = 10;
  // Disk cache is permanent; only forceReload (manual refresh) clears it.

  Future<String> _diskCachePath(String cacheKey) async {
    final dir = await getApplicationDocumentsDirectory();
    final cacheDir = Directory(p.join(dir.path, '.romcache'));
    await cacheDir.create(recursive: true);
    // Sanitize key for use as a filename
    final safeKey = cacheKey.replaceAll(RegExp(r'[^a-zA-Z0-9_-]'), '_');
    return p.join(cacheDir.path, '$safeKey.json');
  }

  Future<List<RomModel>?> _readDiskCache(String cacheKey) async {
    try {
      final path = await _diskCachePath(cacheKey);
      final file = File(path);
      if (!await file.exists()) return null;
      final raw = json.decode(await file.readAsString()) as List<dynamic>;
      return raw.map((e) => RomModel(
        filename: e['filename'] as String,
        size: e['size'] as String? ?? 'N/A',
      )).toList();
    } catch (_) {
      return null;
    }
  }

  Future<void> _writeDiskCache(String cacheKey, List<RomModel> roms) async {
    try {
      final path = await _diskCachePath(cacheKey);
      final data = roms.map((r) => {'filename': r.filename, 'size': r.size}).toList();
      await File(path).writeAsString(json.encode(data));
    } catch (_) {
      // Non-fatal, just means next launch fetches fresh
    }
  }

  /// Silently re-fetches a console's list in the background after serving
  /// cached data instantly. fetchFileList(forceReload: true) internally
  /// updates both in-memory and disk cache with the fresh full list.
  /// Never blocks the UI, never throws to the caller.
  Future<void> _backgroundRefresh(
    String category,
    String consoleKey,
    bool onlyRa,
    String cacheKey,
    List<RomModel> cached,
  ) async {
    try {
      final fresh = await fetchFileList(category, consoleKey, forceReload: true, onlyRa: onlyRa);
      final cachedNames = cached.map((r) => r.filename).toSet();
      final newOnes = fresh.where((r) => !cachedNames.contains(r.filename)).toList();
      if (newOnes.isNotEmpty) {
        _log.info('Background refresh found ' + newOnes.length.toString() + ' new entries for ' + cacheKey);
      }
    } catch (e) {
      _log.warning('Background refresh failed for ' + cacheKey + ': ' + e.toString());
    }
  }

  Future<List<RomModel>> fetchFileList(
    String category,
    String consoleKey, {
    bool forceReload = false,
    bool onlyRa = true,
  }) async {
    final cacheKey = '${category}_${consoleKey}_${onlyRa ? 'ra' : 'alt'}';

    // 1. In-memory cache (instant, session-only)
    final entry = _cache[cacheKey];
    if (!forceReload && entry != null && !entry.isExpired) {
      return entry.data;
    }

    // 2. Disk cache (permanent, instant after first load).
    // Return it immediately, then silently refresh in the background so any
    // newly-added games on the source show up next time without blocking the UI.
    if (!forceReload) {
      final diskHit = await _readDiskCache(cacheKey);
      if (diskHit != null) {
        _cache[cacheKey] = _CacheEntry(diskHit);
        unawaited(_backgroundRefresh(category, consoleKey, onlyRa, cacheKey, diskHit));
        return diskHit;
      }
    }

    final config = _configService.getConsoleConfig(category, consoleKey);
    if (config == null) throw Exception('Console config not found');

    // When unchecked, use the No-Intro/Redump alt source if this console has
    // one configured. If it doesn't, fall back to the RA-curated source
    // rather than returning nothing.
    final bool useAlt = !onlyRa && config['alt_url'] != null;
    // Unchecked but this console has no alt source configured yet:
    // show nothing instead of silently falling back to the RA list.
    if (!onlyRa && config['alt_url'] == null) {
      _cache[cacheKey] = _CacheEntry(const []);
      return const [];
    }
    final dynamic urlField = useAlt ? config['alt_url'] : config['url'];

    // Multi-source merge: url is a JSON array of archive.org item base URLs.
    if (urlField is List) {
      final List<RomModel> mergedRoms = [];
      final List<dynamic> mergedExts = useAlt ? (config['alt_exts'] ?? config['exts']) : config['exts'];
      final mergedValidExts = mergedExts.map((e) => e.toString().toLowerCase()).toList();
      for (final rawUrl in urlField) {
        final itemUrl = rawUrl.toString();
        try {
          final uri = Uri.parse(itemUrl);
          final parts = uri.pathSegments.where((s) => s.isNotEmpty).toList();
          if (parts.isEmpty || parts[0] != 'download') continue;
          final itemId = parts[1];
          final subfolder = parts.length > 2 ? parts.sublist(2).join('/') : '';
          final metaResponse = await http.get(Uri.parse('https://archive.org/metadata/$itemId'));
          if (metaResponse.statusCode != 200) continue;
          final meta = json.decode(metaResponse.body);
          final files = meta['files'] as List<dynamic>? ?? [];
          for (final file in files) {
            final name = file['name'] as String? ?? '';
            final lowerName = name.toLowerCase();
            if (!mergedValidExts.any((ext) => lowerName.endsWith(ext))) continue;
            if (subfolder.isNotEmpty && !name.startsWith(subfolder)) continue;
            final relativePath = subfolder.isNotEmpty ? name.substring(subfolder.length + 1) : name;
            if (relativePath.isEmpty) continue;
            final sizeStr = file['size'] as String? ?? '0';
            final size = int.tryParse(sizeStr) ?? 0;
            mergedRoms.add(RomModel(filename: 'https://archive.org/download/$itemId/$relativePath', size: size.toString()));
          }
        } catch (e) {
          _log.warning('Skipping source \$itemUrl: \$e');
        }
      }
      if (_cache.length >= _maxCacheEntries) {
        final oldest = _cache.entries.reduce(
          (a, b) => a.value.createdAt.isBefore(b.value.createdAt) ? a : b,
        );
        _cache.remove(oldest.key);
      }
      _cache[cacheKey] = _CacheEntry(mergedRoms);
      return mergedRoms;
    }

    final String url = urlField as String;
    if (url.contains('lolroms.com')) {
      final cacheLolKey = '${category}_$consoleKey';
      final cachedEntry = _cache[cacheLolKey];
      if (!forceReload && cachedEntry != null && !cachedEntry.isExpired) {
        return cachedEntry.data;
      }
      final List<dynamic> lolExts = config['exts'];
      final lolValidExts = lolExts.map((e) => e.toString().toLowerCase()).toList();
      final List<RomModel> lolRoms = [];
      final links = await LolromsScraper.fetchLinks(url);
      for (final item in links) {
        final href = item['href'] ?? '';
        final lowerHref = href.toLowerCase();
        if (lolValidExts.any((ext) => lowerHref.endsWith(ext))) {
          final filename = Uri.decodeComponent(href.split('/').last);
          if (filename == '.' || filename == '..' || filename.isEmpty) continue;
          lolRoms.add(RomModel(filename: filename, size: item['size'] ?? 'N/A'));
        }
      }
      _cache[cacheLolKey] = _CacheEntry(lolRoms);
      return lolRoms;
    }
    final List<dynamic> exts = useAlt ? (config['alt_exts'] ?? config['exts']) : config['exts'];
    final validExts = exts.map((e) => e.toString().toLowerCase()).toList();

    try {
      _log.info('Fetching ROM list from: $url');
      final headers = <String, String>{};
      final iaCookie = ArchiveAuthService.instance.cookieHeader;
      if (iaCookie != null && url.contains('archive.org')) {
        headers['Cookie'] = iaCookie;
      }
      final response = await http.get(Uri.parse(url), headers: headers);

      if (response.statusCode != 200) {
        throw Exception('HTTP Error ${response.statusCode}');
      }

      final List<RomModel> roms = [];

      if (url.contains('lolroms.com')) {
        final links = await LolromsScraper.fetchLinks(url);
        for (final item in links) {
          final href = item['href'] ?? '';
          final lowerHref = href.toLowerCase();
          if (validExts.any((ext) => lowerHref.endsWith(ext))) {
            final filename = Uri.decodeComponent(href.split('/').last);
            if (filename == '.' || filename == '..' || filename.isEmpty) continue;
            roms.add(RomModel(filename: filename, size: item['size'] ?? 'N/A'));
          }
        }
      } else if (url.contains('archive.org/download/')) {
        // Archive.org: use metadata JSON API instead of HTML parsing
        final uri = Uri.parse(url);
        final parts = uri.pathSegments.where((s) => s.isNotEmpty).toList();
        // parts[0]='download', parts[1]=itemId, parts[2+]=subfolder
        final itemId = parts[1];
        final subfolder = parts.length > 2 ? parts.sublist(2).join('/') : '';
        final metaUrl = 'https://archive.org/metadata/$itemId';
        final metaHeaders = <String, String>{};
        final iaCookieMeta = ArchiveAuthService.instance.cookieHeader;
        if (iaCookieMeta != null) metaHeaders['Cookie'] = iaCookieMeta;
        final metaResponse = await http.get(Uri.parse(metaUrl), headers: metaHeaders);
        if (metaResponse.statusCode == 200) {
          final meta = json.decode(metaResponse.body);
          final files = meta['files'] as List<dynamic>? ?? [];
          for (final file in files) {
            final name = file['name'] as String? ?? '';
            final lowerName = name.toLowerCase();
            if (!validExts.any((ext) => lowerName.endsWith(ext))) continue;
            // Filter by subfolder if present
            if (subfolder.isNotEmpty && !name.startsWith(subfolder)) continue;
            // Relative path from subfolder root (may still be nested e.g. "FolderName/Game (U).zip")
            final relativePath = subfolder.isNotEmpty ? name.substring(subfolder.length + 1) : name;
            // Display name is always just the file name, no folder
            final displayName = relativePath.split('/').last;
            if (displayName.isEmpty) continue;
            final sizeStr = file['size'] as String? ?? '0';
            final size = int.tryParse(sizeStr) ?? 0;
            // filename stored = relativePath so download URL = baseUrl + relativePath (correct)
            roms.add(RomModel(filename: relativePath, size: size.toString()));
          }
        }
      } else {
        final document = html_parser.parse(response.body);
        final links = document.querySelectorAll('a');
        for (final link in links) {
          final href = link.attributes['href'];
          if (href == null) continue;
          final lowerHref = href.toLowerCase();
          if (validExts.any((ext) => lowerHref.endsWith(ext))) {
            final filename = Uri.decodeComponent(href);
            if (filename == '.' || filename == '..') continue;
            final size = _extractSize(link);
            roms.add(RomModel(filename: filename, size: size));
          }
        }
      }

      // Evict oldest entry if at capacity
      if (_cache.length >= _maxCacheEntries) {
        final oldest = _cache.entries.reduce(
          (a, b) => a.value.createdAt.isBefore(b.value.createdAt) ? a : b,
        );
        _cache.remove(oldest.key);
      }
      _cache[cacheKey] = _CacheEntry(roms);
      // Save permanently to disk
      await _writeDiskCache(cacheKey, roms);
      // Write to disk so next app launch loads instantly
      _writeDiskCache(cacheKey, roms);
      return roms;
    } catch (e) {
      _log.error('Error fetching file list: $e');
      return [];
    }
  }

  String _extractSize(Element link) {
    // Strategy 1: Table based
    final parentTd = link.parent;
    if (parentTd?.localName == 'td') {
      var nextParams = parentTd?.parent?.children;
      if (nextParams != null) {
        for (var td in nextParams) {
          final text = td.text.trim();
          if (RegExp(
                r'\d+(\.\d+)?\s*[BKMG]i?B?',
                caseSensitive: false,
              ).hasMatch(text) &&
              !text.contains('-')) {
            return text;
          }
        }
      }
    }

    // Strategy 2: Text based
    if (link.parentNode != null) {
      final siblings = link.parentNode!.nodes;
      final index = siblings.indexOf(link);
      if (index != -1) {
        for (var i = index + 1; i < siblings.length; i++) {
          final node = siblings[i];
          if (node.nodeType == Node.TEXT_NODE &&
              node.text?.trim().isNotEmpty == true) {
            final parts = node.text!.trim().split(RegExp(r'\s+'));
            if (parts.isNotEmpty) {
              final candidate = parts.last;
              if (RegExp(r'^[\d\.]+[BKMG]$').hasMatch(candidate)) {
                return candidate;
              }
            }
            break;
          }
        }
      }
    }
    return 'N/A';
  }

  Future<List<RomModel>> search(
    String category,
    String consoleKey,
    String query, {
    List<String>? regions,
    List<String>? languages,
    bool hideDemos = true,
    bool hideBetas = true,
    bool hideUnlicensed = true,
    bool onlyRa = true,
  }) async {
    var roms = await fetchFileList(category, consoleKey, onlyRa: onlyRa);
    final activeRegions = regions ?? [];
    final activeLanguages = languages ?? [];
    List<RomModel> filtered = [];

    for (var rom in roms) {
      final filename = rom.filename;

      // 1. Search query filter
      if (query.isNotEmpty) {
        final queryNorm = _normalize(query);
        final filenameNorm = _normalize(rom.displayName);
        if (!filenameNorm.contains(queryNorm)) {
          continue;
        }
      }

      // 2. Region filter (if any regions are selected)
      // Matches: (USA), (Europe), (Japan), (World), etc.
      if (activeRegions.isNotEmpty) {
        bool regionMatch = false;
        for (var r in activeRegions) {
          // Logic: (Region) OR (Region, OR , Region, OR , Region)
          if (filename.contains('($r)') ||
              filename.contains('($r,') ||
              filename.contains(', $r,') ||
              filename.contains(', $r)') ||
              // Also check without space just in case
              filename.contains(',$r,') ||
              filename.contains(',$r)')) {
            regionMatch = true;
            break;
          }
        }
        if (!regionMatch) continue;
      }

      // 3. Language filter (if any languages are selected)
      // Matches: (En), (Fr), (En,Fr,De), (Fr,De,Es,It), etc.
      if (activeLanguages.isNotEmpty) {
        bool languageMatch = false;
        for (var lang in activeLanguages) {
          // Match standalone: (Fr) or at start: (Fr, or in middle: ,Fr, or at end: ,Fr)
          if (filename.contains('($lang)') ||
              filename.contains('($lang,') ||
              filename.contains(',$lang,') ||
              filename.contains(',$lang)')) {
            languageMatch = true;
            break;
          }
        }
        if (!languageMatch) continue;
      }

      // 4. Hide Demos/Samples
      if (hideDemos &&
          (filename.contains('(Demo') || filename.contains('(Sample'))) {
        continue;
      }

      // 5. Hide Betas/Protos
      if (hideBetas &&
          (filename.contains('(Beta') || filename.contains('(Proto'))) {
        continue;
      }

      // 6. Hide Unlicensed
      if (hideUnlicensed && filename.contains('(Unl)')) {
        continue;
      }

      filtered.add(rom);
    }

    // Sort alphabetically
    filtered.sort((a, b) => a.filename.compareTo(b.filename));
    return filtered;
  }

  String _normalize(String text) {
    return text.toLowerCase().replaceAll(RegExp(r'[^a-z0-9]'), '');
  }

  /// Tiny async mutex so concurrent segments never interleave a
  /// setPosition()+writeFrom() pair on the shared RandomAccessFile.
  /// Network reads still overlap freely; only the brief local write is serialized.
  Future<T> _withFileLock<T>(Future<T> Function() action) {
    final previous = _fileLockChain;
    final completer = Completer<void>();
    _fileLockChain = completer.future;
    return previous.then((_) async {
      try {
        return await action();
      } finally {
        completer.complete();
      }
    });
  }
  Future<void> _fileLockChain = Future.value();

  /// Multi-mirror segmented download for Internet Archive sources only.
  /// Splits the file across IA's two independent datacenter mirrors (d1/d2 from
  /// /metadata/<item>) and downloads 4 byte-range segments concurrently.
  /// Throws (without side effects) if ANY eligibility check fails before any
  /// bytes are written, so the caller can silently fall back to single-stream.
  /// Throws AFTER yielding progress only on a genuine mid-transfer I/O failure.
  Stream<DownloadProgressEvent> _segmentedDownload({
    required String downloadUrl,
    required String filename,
    required String consoleKey,
    required String saveDir,
    String? customPath,
    required Map<String, dynamic> config,
    DownloadCancellationToken? cancelToken,
  }) async* {
    final uri = Uri.parse(downloadUrl);
    final parts = uri.pathSegments.where((s) => s.isNotEmpty).toList();
    if (parts.length < 3 || parts[0] != 'download') {
      throw Exception('not a segmentable archive.org URL');
    }
    final itemId = parts[1];
    final encodedRelPath = parts.sublist(2).join('/');

    final metaResp = await http.get(Uri.parse('https://archive.org/metadata/$itemId'));
    if (metaResp.statusCode != 200) throw Exception('metadata fetch failed');
    final meta = json.decode(metaResp.body) as Map<String, dynamic>;
    final dir = meta['dir'] as String?;
    final d1 = meta['d1'] as String?;
    final d2 = meta['d2'] as String?;
    if (dir == null || d1 == null || d2 == null || d1.isEmpty || d2.isEmpty || d1 == d2) {
      throw Exception('mirrors unavailable for $itemId');
    }
    final trimmedDir = dir.endsWith('/') ? dir.substring(0, dir.length - 1) : dir;
    final url1 = 'https://$d1$trimmedDir/$encodedRelPath';
    final url2 = 'https://$d2$trimmedDir/$encodedRelPath';

    final probeResp = await http.get(Uri.parse(url1), headers: {'Range': 'bytes=0-0'});
    if (probeResp.statusCode != 206) throw Exception('range not supported on $url1');
    final contentRange = probeResp.headers['content-range'];
    final totalSize = contentRange != null ? (int.tryParse(contentRange.split('/').last) ?? 0) : 0;
    if (totalSize < 2 * 1024 * 1024) throw Exception('file too small to benefit from segmentation');

    final String saveFilename = Uri.decodeComponent(filename.split('/').last);
    final String finalPath = (customPath != null && customPath.isNotEmpty)
        ? p.join(customPath, saveFilename)
        : p.join(saveDir, config['folder'] ?? consoleKey, saveFilename);

    if (await File(finalPath).exists()) {
      yield DownloadProgressEvent(progress: 1.0, receivedBytes: totalSize, totalBytes: totalSize);
      return;
    }

    await Directory(p.dirname(finalPath)).create(recursive: true);
    final tmpFile = File('$finalPath.tmp');
    final raf = await tmpFile.open(mode: FileMode.write);
    await raf.truncate(totalSize);

    const segCount = 4;
    final segSize = (totalSize / segCount).ceil();
    final mirrors = [url1, url2, url1, url2];
    final receivedPerSeg = List<int>.filled(segCount, 0);
    bool cancelled = false;
    cancelToken?.onCancel(() => cancelled = true);

    final controller = StreamController<DownloadProgressEvent>();
    int totalReceived() => receivedPerSeg.fold(0, (a, b) => a + b);
    Timer? ticker = Timer.periodic(const Duration(milliseconds: 300), (_) {
      if (!controller.isClosed) {
        controller.add(DownloadProgressEvent(
          progress: totalReceived() / totalSize,
          receivedBytes: totalReceived(),
          totalBytes: totalSize,
        ));
      }
    });

    Future<void> downloadSegment(int index) async {
      final start = index * segSize;
      final end = ((index + 1) * segSize - 1).clamp(0, totalSize - 1);
      if (start > end) return;
      final mirrorUrl = mirrors[index % mirrors.length];
      final req = http.Request('GET', Uri.parse(mirrorUrl));
      req.headers['Range'] = 'bytes=$start-$end';
      req.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)';
      final segClient = http.Client();
      try {
        final response = await segClient.send(req);
        if (response.statusCode != 206) throw Exception('segment $index: HTTP ${response.statusCode}');
        int offset = start;
        await for (final chunk in response.stream) {
          if (cancelled) throw Exception('cancelled');
          await _withFileLock(() async {
            await raf.setPosition(offset);
            await raf.writeFrom(chunk);
          });
          offset += chunk.length;
          receivedPerSeg[index] = offset - start;
        }
      } finally {
        segClient.close();
      }
    }

    Future<void> runAll() async {
      try {
        await Future.wait(List.generate(segCount, downloadSegment));
      } finally {
        ticker?.cancel();
        await controller.close();
      }
    }

    try {
      final runFuture = runAll();
      yield* controller.stream;
      await runFuture;
    } finally {
      await raf.close();
    }

    final writtenSize = await tmpFile.length();
    if (writtenSize != totalSize) {
      await tmpFile.delete();
      throw Exception('size mismatch: wrote $writtenSize, expected $totalSize');
    }
    await tmpFile.rename(finalPath);
    yield DownloadProgressEvent(progress: 1.0, receivedBytes: totalSize, totalBytes: totalSize);
  }

  Stream<DownloadProgressEvent> downloadFile(
    String category,
    String consoleKey,
    String filename, {
    required String saveDir,
    String? customPath,
    DownloadCancellationToken? cancelToken,
    int resumeFrom = 0,
    bool onlyRa = true,
  }) async* {
    final config = _configService.getConsoleConfig(category, consoleKey);
    if (config == null) throw Exception('Config error');

    final bool useAltDl = !onlyRa && config['alt_url'] != null;
    if (!onlyRa && config['alt_url'] == null) {
      throw Exception('No alternate source configured for this console yet.');
    }
    final dynamic urlFieldDl = useAltDl ? config['alt_url'] : config['url'];
    String baseUrl = (urlFieldDl is List)
        ? (urlFieldDl.isNotEmpty ? urlFieldDl[0].toString() : '')
        : urlFieldDl as String;
    if (!baseUrl.endsWith('/')) baseUrl += '/';
    String downloadUrl;
    if (filename.startsWith('//')) {
      // Protocol-relative absolute URL (e.g. archive.org view_archive.php hrefs)
      final pathPart = filename.substring(2);
      final encodedPath = pathPart.split('/').map((s) => Uri.encodeComponent(s).replaceAll('+', '%20')).join('/');
      downloadUrl = 'https://$encodedPath';
    } else if (filename.startsWith('http://') || filename.startsWith('https://')) {
      final schemeEnd = filename.indexOf('//') + 2;
      final scheme = filename.substring(0, schemeEnd);
      final pathPart = filename.substring(schemeEnd);
      final encodedPath = pathPart.split('/').map((s) => Uri.encodeComponent(s).replaceAll('+', '%20')).join('/');
      downloadUrl = '$scheme$encodedPath';
    } else {
      final encodedName = filename.split('/').map((s) => Uri.encodeComponent(s).replaceAll('+', '%20')).join('/');
      downloadUrl = '$baseUrl$encodedName';
    }

    // Check if we're using SAF (content:// URI)
    final bool useSaf = _configService.isSafUri(saveDir);

    _log.info(
      'Downloading: $downloadUrl${resumeFrom > 0 ? ' (resuming from $resumeFrom bytes)' : ''}',
    );
    _log.info('Save dir: $saveDir (SAF: $useSaf)');

    // Try IA multi-mirror segmented download first. Any failure before bytes
    // are written falls through silently to the proven single-stream path.
    if (!useSaf && resumeFrom == 0 && downloadUrl.contains('archive.org/download/')) {
      bool segStarted = false;
      try {
        await for (final progress in _segmentedDownload(
          downloadUrl: downloadUrl,
          filename: filename,
          consoleKey: consoleKey,
          saveDir: saveDir,
          customPath: customPath,
          config: config,
          cancelToken: cancelToken,
        )) {
          segStarted = true;
          yield progress;
        }
        return;
      } catch (e) {
        if (segStarted) {
          _log.warning('Segmented download failed mid-transfer, falling back to single-stream: $e');
        } else {
          _log.info('Segmented download not eligible: $e');
        }
      }
    }

    // HTTP client with timeouts to detect dead connections
    final rawHttpClient = HttpClient()
      ..connectionTimeout = const Duration(seconds: 30)
      ..idleTimeout = const Duration(seconds: 60);
    final client = IOClient(rawHttpClient);

    // Register cancellation
    cancelToken?.onCancel(() {
      _log.info('Download cancelled: $filename');
      client.close();
    });

    try {
      final request = http.Request('GET', Uri.parse(downloadUrl));
      request.headers.addAll({
        'User-Agent': downloadUrl.contains('lolroms.com')
            ? (LolromsScraper.cachedUserAgent ?? 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)')
            : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer': baseUrl,
      });
      if (downloadUrl.contains('lolroms.com') && LolromsScraper.cachedCookieHeader != null) {
        request.headers['Cookie'] = LolromsScraper.cachedCookieHeader!;
      }
      final iaCookieDownload = ArchiveAuthService.instance.cookieHeader;
      if (iaCookieDownload != null && downloadUrl.contains('archive.org')) {
        request.headers['Cookie'] = iaCookieDownload;
      }
      if (resumeFrom > 0 && !useSaf) {
        request.headers['Range'] = 'bytes=$resumeFrom-';
      }

      final response = await client.send(request);

      // Handle resume: server returns 206 for Range requests, 200 for full
      int totalLength;
      int received;
      if (resumeFrom > 0 && response.statusCode == 206) {
        totalLength = (response.contentLength ?? 0) + resumeFrom;
        received = resumeFrom;
        _log.info('Resume accepted: continuing from $resumeFrom bytes');
      } else {
        totalLength = response.contentLength ?? 0;
        received = 0;
        if (resumeFrom > 0) {
          _log.warning(
            'Server ignored Range header (status ${response.statusCode}), restarting from 0',
          );
        }
      }

      // Clean save name: always last path segment, decoded.
      // Handles plain filenames, nested archive.org relative paths,
      // and absolute archive.org view_archive.php URLs the same way.
      final String saveFilename = Uri.decodeComponent(filename.split('/').last);

      if (useSaf) {
        // === SAF PATH (Android SD Card) ===
        await for (final progress in _downloadWithSaf(
          response.stream,
          saveDir,
          config['folder'] ?? consoleKey,
          saveFilename,
          totalLength,
          cancelToken,
        )) {
          yield progress;
        }
      } else {
        // === REGULAR PATH (Internal storage / Desktop) ===
        final String finalPath;
        if (customPath != null && customPath.isNotEmpty) {
          finalPath = p.join(customPath, saveFilename);
        } else {
          finalPath = p.join(saveDir, config['folder'] ?? consoleKey, saveFilename);
        }

        await Directory(p.dirname(finalPath)).create(recursive: true);

        if (await File(finalPath).exists()) {
          yield DownloadProgressEvent(
            progress: 1.0,
            receivedBytes: totalLength,
            totalBytes: totalLength,
          );
          return;
        }

        final file = File('$finalPath.tmp');
        IOSink? sink;

        try {
          // Append mode if resuming, write mode otherwise
          if (resumeFrom > 0 && await file.exists()) {
            sink = file.openWrite(mode: FileMode.append);
          } else {
            sink = file.openWrite();
          }
          int lastReportedBytes = received;

          await for (final chunk in response.stream) {
            if (cancelToken?.isCancelled ?? false) {
              throw Exception('Download cancelled');
            }
            sink.add(chunk);
            received += chunk.length;

            // Throttle: Update only every 100KB
            if (totalLength > 0 &&
                (received - lastReportedBytes > 1024 * 100 ||
                    received == totalLength)) {
              yield DownloadProgressEvent(
                progress: received / totalLength,
                receivedBytes: received,
                totalBytes: totalLength,
              );
              lastReportedBytes = received;
            }
          }
          await sink.close();
          sink = null;

          // Verify download completeness
          if (totalLength > 0 && received != totalLength) {
            throw IncompleteDownloadException(
              received: received,
              expected: totalLength,
              tempFilePath: file.path,
            );
          }

          await file.rename(finalPath);
        } catch (e) {
          await sink?.close();
          // Preserve .tmp for resume on incomplete downloads
          if (e is! IncompleteDownloadException && await file.exists()) {
            try {
              await file.delete();
              _log.info('Deleted incomplete file: ${file.path}');
            } catch (delError) {
              _log.warning('Failed to delete incomplete file: $delError');
            }
          }
          rethrow;
        }

        // Handle zip extraction for regular paths
        if (filename.toLowerCase().endsWith('.zip')) {
          yield DownloadProgressEvent(
            progress: 1.01,
            receivedBytes: totalLength,
            totalBytes: totalLength,
            phase: 'extracting',
          );
          try {
            await for (final progress in _extractZipStream(finalPath)) {
              if (cancelToken?.isCancelled ?? false)
                throw Exception('Cancelled during extraction');
              // Offset so progress is always > 1.0 (never exactly 1.0)
              yield DownloadProgressEvent(
                progress: 1.0 + (progress * 0.99) + 0.01,
                receivedBytes: totalLength,
                totalBytes: totalLength,
                phase: 'extracting',
              );
            }
          } catch (e) {
            _log.warning('Extraction failed: $e');
            throw ExtractionException(e.toString());
          }
        }
      }
    } catch (e) {
      if (cancelToken?.isCancelled ?? false) {
        _log.info('Clean cancellation handled');
        throw Exception('Download cancelled');
      }
      rethrow;
    } finally {
      client.close();
    }
  }

  /// Safe mkdirp that handles pre-existing directories gracefully
  Future<dynamic> _safeMkdirp(
    SafUtil safUtil,
    String baseUri,
    List<String> segments,
  ) async {
    try {
      // Try direct creation first (fast path)
      return await safUtil.mkdirp(baseUri, segments);
    } catch (e) {
      // If it fails, fallback to checking existence segment by segment
      if (segments.isEmpty) {
        rethrow;
      }

      final currentSegment = segments.first;
      dynamic match;

      try {
        final children = await safUtil.list(baseUri);
        match = children.firstWhere(
          (element) =>
              element.name.toLowerCase() == currentSegment.toLowerCase(),
          orElse: () =>
              throw Exception('Segment not found'), // Trigger outer catch
        );
      } catch (_) {
        // Not found in list, and mkdirp failed? Real error.
        _log.error(
          'mkdirp failed and segment "$currentSegment" not found in $baseUri',
        );
        rethrow;
      }

      // Found the segment!
      if (segments.length == 1) {
        // It was the last one, success!
        return match;
      } else {
        // Recurse for remaining segments
        return _safeMkdirp(safUtil, match.uri, segments.sublist(1));
      }
    }
  }

  /// Download using SAF for Android SD card access
  /// For ZIPs: download to temp, extract, paste to SAF
  Stream<DownloadProgressEvent> _downloadWithSaf(
    Stream<List<int>> responseStream,
    String safDirUri,
    String subFolder,
    String filename,
    int totalLength,
    DownloadCancellationToken? cancelToken,
  ) async* {
    final safStream = SafStream();
    final safUtil = SafUtil();
    int received = 0;
    final bool isZip = filename.toLowerCase().endsWith('.zip');

    try {
      // Create subfolder if it doesn't exist
      final subDirResult = await _safeMkdirp(safUtil, safDirUri, [subFolder]);
      final targetDirUri = subDirResult.uri;

      if (isZip) {
        // === ZIP HANDLING: Download to temp cache, extract, paste to SAF ===
        _log.info('ZIP detected - using temp cache extraction method');

        // Get temp directory for extraction
        final tempDir = await Directory.systemTemp.createTemp('romifleur_zip_');
        final tempZipPath = p.join(tempDir.path, filename);

        try {
          // Download to temp file
          final tempFile = File(tempZipPath);
          final sink = tempFile.openWrite();
          int lastReportedBytes = 0;

          await for (final chunk in responseStream) {
            if (cancelToken?.isCancelled ?? false) {
              await sink.close();
              throw Exception('Download cancelled');
            }
            sink.add(chunk);
            received += chunk.length;

            // Throttle: Update only every 100KB
            if (totalLength > 0 &&
                (received - lastReportedBytes > 1024 * 100 ||
                    received == totalLength)) {
              yield DownloadProgressEvent(
                progress: received / totalLength * 0.8,
                receivedBytes: received,
                totalBytes: totalLength,
                phase: 'download',
              ); // 0-80%
              lastReportedBytes = received;
            }
          }
          await sink.close();

          // Verify download completeness
          if (totalLength > 0 && received != totalLength) {
            throw IncompleteDownloadException(
              received: received,
              expected: totalLength,
            );
          }

          try {
            _log.info('ZIP downloaded to temp: $tempZipPath');
            yield DownloadProgressEvent(
              progress: 0.8,
              receivedBytes: totalLength,
              totalBytes: totalLength,
              phase: 'extracting',
            ); // 80% - download complete, extraction starting

            // Extract ZIP locally (Background Isolate with Granular Progress)
            final receivePort = ReceivePort();
            final isolate = await Isolate.spawn(_isolateExtraction, [
              tempZipPath,
              tempDir.path,
              receivePort.sendPort,
            ]);

            // Detect unexpected isolate exit (e.g. OOM on large PS2 ZIPs)
            isolate.addOnExitListener(
              receivePort.sendPort,
              response: '__isolate_exit__',
            );

            try {
              await for (final message in receivePort) {
                if (message == '__isolate_exit__') {
                  throw Exception(
                    'Extraction failed: isolate exited unexpectedly (possible out-of-memory)',
                  );
                } else if (message is double) {
                  // Map extraction progress (0.0-1.0) to overall progress (0.8-0.9)
                  yield DownloadProgressEvent(
                    progress: 0.8 + (0.1 * message),
                    receivedBytes: totalLength,
                    totalBytes: totalLength,
                    phase: 'extracting',
                  );
                } else if (message == true) {
                  break; // Done
                } else if (message is String) {
                  throw Exception(message);
                }
              }
            } finally {
              isolate.kill(priority: Isolate.immediate);
              receivePort.close();
            }

            _log.info('ZIP extracted locally');
            yield DownloadProgressEvent(
              progress: 0.9,
              receivedBytes: totalLength,
              totalBytes: totalLength,
              phase: 'extracting',
            ); // 90% - extraction complete

            // Delete the ZIP file from temp
            await tempFile.delete();

            // Paste all extracted files to SAF (byte-level progress)
            final extractedFiles = tempDir.listSync(recursive: true);

            // Calculate total bytes for copy progress
            int totalExtractedBytes = 0;
            for (final e in extractedFiles) {
              if (e is File) totalExtractedBytes += e.lengthSync();
            }
            int copiedBytes = 0;
            int lastCopyReportMs = 0;

            for (final entity in extractedFiles) {
              if (entity is File) {
                final relativePath = p.relative(
                  entity.path,
                  from: tempDir.path,
                );
                // Skip the original zip if it somehow exists
                if (relativePath == filename) continue;

                // Create parent dirs in SAF if needed
                final parentDir = p.dirname(relativePath);
                String destDirUri = targetDirUri;
                if (parentDir != '.' && parentDir.isNotEmpty) {
                  final subDirs = parentDir.split(p.separator);
                  final parentResult = await _safeMkdirp(
                    safUtil,
                    targetDirUri,
                    subDirs,
                  );
                  destDirUri = parentResult.uri;
                }

                // Paste file to SAF manually (stream copy) to avoid API issues
                final localFileStream = entity.openRead();
                final copyWriteInfo = await safStream.startWriteStream(
                  destDirUri,
                  p.basename(entity.path),
                  'application/octet-stream',
                );
                final copySessionId = copyWriteInfo.session;

                try {
                  final buffer = BytesBuilder(copy: false);
                  const int bufferSize = 1024 * 1024; // 1MB buffer

                  await for (final chunk in localFileStream) {
                    buffer.add(chunk);
                    if (buffer.length >= bufferSize) {
                      final bytes = buffer.takeBytes();
                      await safStream.writeChunk(copySessionId, bytes);
                      copiedBytes += bytes.length;

                      // Byte-level copy progress (throttle 250ms)
                      final nowMs = DateTime.now().millisecondsSinceEpoch;
                      if (totalExtractedBytes > 0 &&
                          nowMs - lastCopyReportMs > 250) {
                        yield DownloadProgressEvent(
                          progress:
                              0.9 + (0.1 * copiedBytes / totalExtractedBytes),
                          receivedBytes: totalLength,
                          totalBytes: totalLength,
                          phase: 'copying',
                        );
                        lastCopyReportMs = nowMs;
                      }
                    }
                  }
                  if (buffer.isNotEmpty) {
                    final bytes = buffer.takeBytes();
                    await safStream.writeChunk(copySessionId, bytes);
                    copiedBytes += bytes.length;
                  }
                  await safStream.endWriteStream(copySessionId);
                } catch (e) {
                  try {
                    await safStream.endWriteStream(copySessionId);
                  } catch (_) {}
                  rethrow;
                }
              }
            }

            // Cleanup temp directory
            await tempDir.delete(recursive: true);
            _log.info('SAF extraction complete, temp cleaned up');
            yield DownloadProgressEvent(
              progress: 1.0,
              receivedBytes: totalLength,
              totalBytes: totalLength,
              phase: 'copying',
            );
          } catch (e) {
            // New: Wrap any extraction/copy error in ExtractionException to prevent retry
            throw ExtractionException(e.toString());
          }
        } catch (e) {
          // Cleanup temp on error
          try {
            await tempDir.delete(recursive: true);
          } catch (_) {}
          rethrow;
        }
      } else {
        // === NON-ZIP: Direct streaming to SAF ===
        String? sessionId;

        try {
          // Start write stream
          final writeInfo = await safStream.startWriteStream(
            targetDirUri,
            filename,
            'application/octet-stream',
          );
          sessionId = writeInfo.session;

          _log.debug('SAF write session started: $sessionId');

          // Stream download chunks directly to SAF
          final buffer = BytesBuilder(copy: false);
          const int bufferSize = 1024 * 1024; // 1MB buffer
          int lastReportedBytes = 0;

          // Producer-Consumer Logic to decouple Network (Fast) from Disk (Slow-ish)
          final writeController = StreamController<Uint8List>();
          final writeFuture = (() async {
            try {
              await for (final chunk in writeController.stream) {
                await safStream.writeChunk(sessionId!, chunk);
              }
            } catch (e) {
              // If write fails, we should probably propagate?
              // For now, caller will handle main error, this just stops writing.
              _log.error('SAF Async Write Error: $e');
              rethrow;
            }
          })();

          try {
            await for (final chunk in responseStream) {
              if (cancelToken?.isCancelled ?? false) {
                throw Exception('Download cancelled');
              }

              buffer.add(chunk);
              received += chunk.length;

              if (buffer.length >= bufferSize) {
                writeController.add(buffer.takeBytes());
              }

              // Throttle: Update only every 100KB
              if (totalLength > 0 &&
                  (received - lastReportedBytes > 1024 * 100 ||
                      received == totalLength)) {
                yield DownloadProgressEvent(
                  progress: received / totalLength,
                  receivedBytes: received,
                  totalBytes: totalLength,
                );
                lastReportedBytes = received;
              }
            }

            // Verify download completeness
            if (totalLength > 0 && received != totalLength) {
              throw IncompleteDownloadException(
                received: received,
                expected: totalLength,
              );
            }

            if (buffer.isNotEmpty) {
              writeController.add(buffer.takeBytes());
            }
          } catch (e) {
            await writeController.close();
            try {
              await writeFuture;
            } catch (_) {} // Drain pending writes before cleanup
            rethrow;
          }

          // End write stream
          await writeController.close();
          await writeFuture; // Wait for pending writes to finish

          await safStream.endWriteStream(sessionId);
          sessionId = null;

          _log.info('SAF download complete: $filename');
          yield DownloadProgressEvent(
            progress: 1.0,
            receivedBytes: totalLength,
            totalBytes: totalLength,
          );
        } catch (e) {
          // Try to clean up session if it was started
          if (sessionId != null) {
            try {
              await safStream.endWriteStream(sessionId);
            } catch (_) {}
          }
          rethrow;
        }
      }
    } catch (e) {
      // Detect SAF permission errors and wrap them
      final errStr = e.toString().toLowerCase();
      if (errStr.contains('securityexception') ||
          errStr.contains('permission denial') ||
          errStr.contains('eacces') ||
          (errStr.contains('permission') && errStr.contains('denied'))) {
        throw SafPermissionException(uri: safDirUri, message: e.toString());
      }
      rethrow;
    }
  }

  Stream<double> _extractZipStream(String zipPath) async* {
    final receivePort = ReceivePort();
    Isolate? isolate;

    try {
      final dir = p.dirname(zipPath);

      // Spawn the isolation
      isolate = await Isolate.spawn(_isolateExtraction, [
        zipPath,
        dir,
        receivePort.sendPort,
      ]);

      // Detect unexpected isolate exit (e.g. OOM on large ZIPs)
      isolate.addOnExitListener(
        receivePort.sendPort,
        response: '__isolate_exit__',
      );

      // Listen for progress messages
      await for (final message in receivePort) {
        if (message == '__isolate_exit__') {
          throw Exception(
            'Extraction failed: isolate exited unexpectedly (possible out-of-memory)',
          );
        } else if (message is double) {
          yield message; // 0.0 to 1.0
        } else if (message == true) {
          break; // Done
        } else if (message is String) {
          throw Exception(message); // Error from isolate
        }
      }

      // Kill isolate to release file locks on Windows
      isolate.kill(priority: Isolate.immediate);
      isolate = null;

      // Delete zip after successful extraction
      await File(zipPath).delete();
    } catch (e) {
      _log.error('Extraction failed: $e');
      rethrow;
    } finally {
      isolate?.kill(priority: Isolate.immediate);
      receivePort.close();
    }
  }
}
