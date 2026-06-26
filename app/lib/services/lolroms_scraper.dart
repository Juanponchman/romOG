import 'dart:async';
import 'dart:convert';
import 'package:flutter_inappwebview/flutter_inappwebview.dart';

class LolromsScraper {
  static String? cachedCookieHeader;
  static String? cachedUserAgent;

  static Future<List<Map<String, String>>> fetchLinks(String url) async {
    final completer = Completer<List<Map<String, String>>>();
    HeadlessInAppWebView? headless;
    Timer? timeoutTimer;

    headless = HeadlessInAppWebView(
      initialUrlRequest: URLRequest(url: WebUri(url)),
      onLoadStop: (controller, loadedUrl) async {
        await Future.delayed(const Duration(seconds: 3));
        try {
          final result = await controller.evaluateJavascript(source: '''
            JSON.stringify(Array.from(document.querySelectorAll('a[href]')).map(a => {
              let size = '';
              let tr = a.closest('tr');
              if (tr) {
                let tds = tr.querySelectorAll('td');
                for (const td of tds) {
                  if (/\\d+(\\.\\d+)?\\s*[KMGT]?i?B/i.test(td.textContent)) { size = td.textContent.trim(); break; }
                }
              }
              return {href: a.getAttribute('href'), size: size};
            }));
          ''');
          final List<dynamic> items = json.decode(result.toString());
          final links = items
              .map((e) => {'href': (e['href'] ?? '').toString(), 'size': (e['size'] ?? '').toString()})
              .toList();

          // Capture the Cloudflare clearance cookies and matching UA for downloads.
          final cookies = await CookieManager.instance().getCookies(url: WebUri(url));
          cachedCookieHeader = cookies.map((c) => '${c.name}=${c.value}').join('; ');
          cachedUserAgent = await controller.evaluateJavascript(source: 'navigator.userAgent') as String?;

          if (!completer.isCompleted) completer.complete(links.cast<Map<String, String>>());
        } catch (e) {
          if (!completer.isCompleted) completer.complete([]);
        }
        await headless?.dispose();
      },
    );

    await headless.run();
    timeoutTimer = Timer(const Duration(seconds: 20), () {
      if (!completer.isCompleted) completer.complete([]);
      headless?.dispose();
    });
    final result = await completer.future;
    timeoutTimer.cancel();
    return result;
  }
}
