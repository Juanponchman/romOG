import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:romifleur/services/archive_auth_service.dart';
import '../config/theme.dart';

class ArchiveLoginScreen extends StatefulWidget {
  const ArchiveLoginScreen({super.key});
  @override
  State<ArchiveLoginScreen> createState() => _ArchiveLoginScreenState();
}

class _ArchiveLoginScreenState extends State<ArchiveLoginScreen> {
  final _emailController = TextEditingController();
  final _passwordController = TextEditingController();
  bool _loading = false;
  bool _obscurePassword = true;
  bool _rememberMe = false;
  String? _error;

  static const _kSavedEmailKey = 'ia_saved_email';
  static const _kRememberMeKey = 'ia_remember_me';

  @override
  void initState() {
    super.initState();
    _loadSaved();
  }

  Future<void> _loadSaved() async {
    final prefs = await SharedPreferences.getInstance();
    final savedEmail = prefs.getString(_kSavedEmailKey) ?? '';
    final remember = prefs.getBool(_kRememberMeKey) ?? false;
    setState(() {
      _emailController.text = savedEmail;
      _rememberMe = remember;
    });
  }

  @override
  void dispose() {
    _emailController.dispose();
    _passwordController.dispose();
    super.dispose();
  }

  Future<void> _login() async {
    final email = _emailController.text.trim();
    final password = _passwordController.text;
    if (email.isEmpty || password.isEmpty) {
      setState(() => _error = 'Enter email and password');
      return;
    }
    setState(() { _loading = true; _error = null; });

    final error = await ArchiveAuthService.instance.login(email, password);

    if (error == null) {
      final prefs = await SharedPreferences.getInstance();
      await prefs.setBool(_kRememberMeKey, _rememberMe);
      if (_rememberMe) {
        await prefs.setString(_kSavedEmailKey, email);
      } else {
        await prefs.remove(_kSavedEmailKey);
      }
    }

    setState(() { _loading = false; _error = error; });
    if (error == null && mounted) Navigator.of(context).pop(true);
  }

  Future<void> _logout() async {
    await ArchiveAuthService.instance.logout();
    setState(() {});
  }

  @override
  Widget build(BuildContext context) {
    final loggedIn = ArchiveAuthService.instance.isLoggedIn;
    return Scaffold(
      backgroundColor: AppTheme.backgroundColor,
      appBar: AppBar(
        title: const Text('Internet Archive Account'),
        backgroundColor: AppTheme.sidebarColor,
      ),
      body: SafeArea(
        child: SingleChildScrollView(
          child: Center(
            child: ConstrainedBox(
              constraints: const BoxConstraints(maxWidth: 460),
              child: Padding(
                padding: const EdgeInsets.all(24),
                child: loggedIn ? _loggedInView() : _loginForm(),
              ),
            ),
          ),
        ),
      ),
    );
  }

  Widget _loggedInView() {
    return Column(
      mainAxisAlignment: MainAxisAlignment.center,
      children: [
        const Icon(Icons.check_circle, color: AppTheme.accentColor, size: 56),
        const SizedBox(height: 16),
        const Text('Signed in to Internet Archive', style: TextStyle(fontSize: 18)),
        const SizedBox(height: 8),
        const Text(
          'Login-gated sources will now download normally.',
          style: TextStyle(color: AppTheme.textMuted, fontSize: 13),
          textAlign: TextAlign.center,
        ),
        const SizedBox(height: 32),
        OutlinedButton(onPressed: _logout, child: const Text('Log Out')),
      ],
    );
  }

  Widget _loginForm() {
    return Card(
      color: AppTheme.cardColor,
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
      child: Padding(
        padding: const EdgeInsets.all(24),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            const Icon(Icons.lock_outline, size: 44, color: AppTheme.primaryColor),
            const SizedBox(height: 12),
            const Text('Account Sign-In', style: TextStyle(fontSize: 20, fontWeight: FontWeight.w600)),
            const SizedBox(height: 4),
            const Text(
              'Sign in to download from login-gated sources',
              style: TextStyle(fontSize: 13, color: AppTheme.textMuted),
              textAlign: TextAlign.center,
            ),
            const SizedBox(height: 20),
            TextField(
              controller: _emailController,
              decoration: const InputDecoration(
                labelText: 'Email address',
                helperText: 'Case-sensitive, must match your account exactly',
                border: OutlineInputBorder(),
              ),
              keyboardType: TextInputType.emailAddress,
              textInputAction: TextInputAction.next,
            ),
            const SizedBox(height: 12),
            TextField(
              controller: _passwordController,
              decoration: InputDecoration(
                labelText: 'Password',
                border: const OutlineInputBorder(),
                suffixIcon: IconButton(
                  icon: Icon(_obscurePassword ? Icons.visibility_off : Icons.visibility),
                  onPressed: () => setState(() => _obscurePassword = !_obscurePassword),
                ),
              ),
              obscureText: _obscurePassword,
              textInputAction: TextInputAction.done,
              onSubmitted: (_) => _login(),
            ),
            Row(
              children: [
                Checkbox(
                  value: _rememberMe,
                  onChanged: (v) => setState(() => _rememberMe = v ?? false),
                ),
                const Text('Remember me'),
              ],
            ),
            if (_error != null) ...[
              const SizedBox(height: 8),
              Text(_error!, style: const TextStyle(color: AppTheme.errorColor), textAlign: TextAlign.center),
            ],
            const SizedBox(height: 8),
            SizedBox(
              width: double.infinity,
              child: FilledButton(
                onPressed: _loading ? null : _login,
                child: _loading
                    ? const SizedBox(height: 20, width: 20, child: CircularProgressIndicator(strokeWidth: 2))
                    : const Text('Log In'),
              ),
            ),
            const SizedBox(height: 8),
            TextButton(
              onPressed: () => Navigator.of(context).pop(false),
              child: const Text('Skip login (public sources only)', style: TextStyle(fontSize: 12)),
            ),
          ],
        ),
      ),
    );
  }
}
