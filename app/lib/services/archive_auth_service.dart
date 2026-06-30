import 'dart:io';
import 'package:dio/dio.dart';
import 'package:dio_cookie_manager/dio_cookie_manager.dart';
import 'package:cookie_jar/cookie_jar.dart';
import 'package:path_provider/path_provider.dart';
import 'package:shared_preferences/shared_preferences.dart';

class ArchiveAuthService {
  static const _kBaseUrl = 'https://archive.org';
  static const _kCookieKey = 'ia_cookie_header';

  static ArchiveAuthService? _instance;
  static ArchiveAuthService get instance => _instance ??= ArchiveAuthService._();
  ArchiveAuthService._();

  Dio? _dio;
  PersistCookieJar? _cookieJar;
  String? _cookieHeader;

  bool get isLoggedIn => _cookieHeader != null && _cookieHeader!.isNotEmpty;
  String? get cookieHeader => _cookieHeader;

  Future<void> loadSavedSession() async {
    final prefs = await SharedPreferences.getInstance();
    final dir = await getApplicationDocumentsDirectory();
    _cookieJar = PersistCookieJar(
      storage: FileStorage('${dir.path}/.ia_cookies/'),
      ignoreExpires: true, // Helpful for long-lived sessions
    );

    _dio = Dio(BaseOptions(
      baseUrl: _kBaseUrl,
      followRedirects: true,
      validateStatus: (s) => s != null && s < 500,
      connectTimeout: const Duration(seconds: 15),
      receiveTimeout: const Duration(seconds: 15),
    ))
      ..interceptors.add(CookieManager(_cookieJar!));

    _cookieHeader = prefs.getString(_kCookieKey);

    // Sync from jar if prefs is empty
    if (_cookieHeader == null || _cookieHeader!.isEmpty) {
      final cookies = await _cookieJar!.loadForRequest(Uri.parse(_kBaseUrl));
      if (cookies.isNotEmpty) {
        _cookieHeader = cookies.map((c) => '\( {c.name}= \){c.value}').join('; ');
        await prefs.setString(_kCookieKey, _cookieHeader!);
      }
    }
  }

  /// Login with email/password. Returns null on success, error string on failure.
  Future<String?> login(String email, String password) async {
    if (_dio == null) await loadSavedSession();

    try {
      // Pre-fetch login page to establish any necessary cookies/CSRF state
      await _dio!.get('/account/login');

      final response = await _dio!.post(
        '/account/login',
        data: {
          'username': email,
          'password': password,
          'remember': 'CHECKED',
          'referer': 'https://archive.org/',
          'login': 'true',
          'submit_by_js': 'true',
        },
        options: Options(
          contentType: Headers.formUrlEncodedContentType,
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            'Referer': 'https://archive.org/account/login',
            'Origin': 'https://archive.org',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          },
        ),
      );

      // Log response for debugging (remove in production)
      print('Login status: ${response.statusCode}');
      print('Response headers: ${response.headers}');

      // Primary check: cookies from jar
      var cookies = await _cookieJar!.loadForRequest(Uri.parse(_kBaseUrl));
      var hasAuth = cookies.any((c) => c.name == 'logged-in-sig' || c.name == 'logged-in-user');

      // Fallback: parse Set-Cookie directly from response
      if (!hasAuth) {
        final rawSetCookie = response.headers.map['set-cookie'];
        if (rawSetCookie != null && rawSetCookie.isNotEmpty) {
          final parsed = rawSetCookie
              .map((raw) => Cookie.fromSetCookieValue(raw))
              .where((c) => c.name == 'logged-in-sig' || c.name == 'logged-in-user')
              .toList();

          if (parsed.isNotEmpty) {
            await _cookieJar!.saveFromResponse(Uri.parse(_kBaseUrl), parsed);
            cookies = await _cookieJar!.loadForRequest(Uri.parse(_kBaseUrl));
            hasAuth = cookies.any((c) => c.name == 'logged-in-sig' || c.name == 'logged-in-user');
          }
        }
      }

      // Additional body check for common failure indicators
      final body = response.data.toString().toLowerCase();
      if (body.contains('bad_login') || body.contains('invalid') || body.contains('failed') || body.contains('incorrect')) {
        return 'Login failed. Check your email and password.';
      }

      if (!hasAuth) {
        return 'Login failed - no authentication cookies received. Check credentials or try again later.';
      }

      // Build cookie header
      _cookieHeader = cookies.map((c) => '\( {c.name}= \){c.value}').join('; ');

      final prefs = await SharedPreferences.getInstance();
      await prefs.setString(_kCookieKey, _cookieHeader!);

      print('Login successful. Cookie header set.');
      return null;
    } catch (e, stack) {
      print('Login error: $e\n$stack');
      if (e is DioException) {
        return 'Network error: ${e.message}';
      }
      return 'Unexpected error: $e';
    }
  }

  Future<void> logout() async {
    _cookieHeader = null;
    await _cookieJar?.deleteAll();
    final prefs = await SharedPreferences.getInstance();
    await prefs.remove(_kCookieKey);
    // Optional: clear Dio instance if needed
    _dio = null;
  }

  // Helper to verify current session (optional)
  Future<bool> verifySession() async {
    if (!isLoggedIn || _dio == null) return false;
    try {
      final response = await _dio!.get('/account/');
      return response.statusCode == 200 && 
             !response.data.toString().contains('sign in') && 
             !response.data.toString().contains('login');
    } catch (e) {
      return false;
    }
  }
}