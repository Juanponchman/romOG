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
    _cookieJar = PersistCookieJar(storage: FileStorage('${dir.path}/.ia_cookies/'));
    _dio = Dio(BaseOptions(baseUrl: _kBaseUrl))
      ..interceptors.add(CookieManager(_cookieJar!));
    _cookieHeader = prefs.getString(_kCookieKey);
    final cookies = await _cookieJar!.loadForRequest(Uri.parse(_kBaseUrl));
    if (cookies.isNotEmpty && _cookieHeader == null) {
      _cookieHeader = cookies.map((c) => '${c.name}=${c.value}').join('; ');
    }
  }

  /// Login with email/password. Returns null on success, error string on failure.
  Future<String?> login(String email, String password) async {
    if (_dio == null) await loadSavedSession();
    try {
      await _dio!.post(
        '/account/login',
        data: 'username=${Uri.encodeComponent(email)}&password=${Uri.encodeComponent(password)}'
            '&remember=CHECKED&referer=https%3A%2F%2Farchive.org%2F&login=true&submit_by_js=true',
        options: Options(
          headers: {'Content-Type': 'application/x-www-form-urlencoded'},
          followRedirects: true,
          validateStatus: (s) => s != null && s < 500,
        ),
      );
      final cookies = await _cookieJar!.loadForRequest(Uri.parse(_kBaseUrl));
      final hasAuth = cookies.any((c) => c.name == 'logged-in-sig');
      if (!hasAuth) return 'Login failed. Check your email and password.';
      _cookieHeader = cookies.map((c) => '${c.name}=${c.value}').join('; ');
      final prefs = await SharedPreferences.getInstance();
      await prefs.setString(_kCookieKey, _cookieHeader!);
      return null;
    } catch (e) {
      return 'Network error: $e';
    }
  }

  Future<void> logout() async {
    _cookieHeader = null;
    await _cookieJar?.deleteAll();
    final prefs = await SharedPreferences.getInstance();
    await prefs.remove(_kCookieKey);
  }
}
