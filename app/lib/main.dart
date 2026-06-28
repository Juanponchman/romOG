import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';

import 'config/theme.dart';
import 'screens/home_screen.dart';
import 'services/archive_auth_service.dart';

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await ArchiveAuthService.instance.loadSavedSession();
  runApp(const ProviderScope(child: RomifleurApp()));
}

class RomifleurApp extends StatelessWidget {
  const RomifleurApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'romØG',
      debugShowCheckedModeBanner: false,
      theme: AppTheme.darkTheme,
      home: const HomeScreen(),
    );
  }
}
