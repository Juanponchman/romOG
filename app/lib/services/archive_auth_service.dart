import 'package:http/http.dart' as http;
import 'package:shared_preferences/shared_preferences.dart';

class ArchiveAuthService {
  static const _kCookieKey = 'ia_cookie_header';

  static ArchiveAuthService? _instance;
  static ArchiveAuthService get instance => _instance ??= ArchiveAuthService._();
  ArchiveAuthService._();

  String? _cookieHeader;

  bool get isLoggedIn => _cookieHeader != null && _cookieHeader!.isNotEmpty;
  String? get cookieHeader => _cookieHeader;

  Future<void> loadSavedSession() async {
    final prefs = await SharedPreferences.getInstance();
    _cookieHeader = prefs.getString(_kCookieKey);
  }

  /// Login with email/password. Returns null on success, error string on failure.
  Future<String?> login(String email, String password) async {
    try {
      final response = await http.post(
        Uri.parse('https://archive.org/account/login'),
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        },
        body: {
          'username': email,
          'password': password,
          'remember': 'CHECKED',
          'referer': 'https://archive.org/',
          'login': 'true',
          'submit_by_js': 'true',
        },
      );

      final rawCookies = response.headers['set-cookie'] ?? '';
      if (rawCookies.isEmpty) {
        return 'Login failed: no cookies received. Check your credentials.';
      }

      final user = _extractCookie(rawCookies, 'logged-in-user');
      final sig = _extractCookie(rawCookies, 'logged-in-sig');

      if (user == null || sig == null) {
        return 'Login failed: invalid credentials or server error.';
      }

      _cookieHeader = 'logged-in-user=$user; logged-in-sig=$sig';

      final prefs = await SharedPreferences.getInstance();
      await prefs.setString(_kCookieKey, _cookieHeader!);

      return null;
    } catch (e) {
      return 'Network error: $e';
    }
  }

  Future<void> logout() async {
    _cookieHeader = null;
    final prefs = await SharedPreferences.getInstance();
    await prefs.remove(_kCookieKey);
  }

  String? _extractCookie(String rawCookies, String name) {
    final match = RegExp('$name=([^;,]+)').firstMatch(rawCookies);
    return match?.group(1);
  }
}
