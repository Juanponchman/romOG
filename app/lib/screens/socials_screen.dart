import 'package:flutter/material.dart';
import 'package:url_launcher/url_launcher.dart';
import '../config/theme.dart';

class SocialsScreen extends StatelessWidget {
  const SocialsScreen({super.key});

  Future<void> _open(String url) async {
    await launchUrl(Uri.parse(url), mode: LaunchMode.externalApplication);
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: Colors.black,
      body: SafeArea(
        child: Column(
          children: [
            AppBar(
              backgroundColor: Colors.transparent,
              elevation: 0,
              leading: IconButton(
                icon: const Icon(Icons.close, color: Colors.white),
                onPressed: () => Navigator.of(context).pop(),
              ),
            ),
            Expanded(
              child: SingleChildScrollView(
                child: Column(
                  children: [
                    Image.asset('assets/socials_splash.jpg', fit: BoxFit.contain),
                    const SizedBox(height: 16),
                    _socialButton(context, Icons.play_circle_fill, 'YouTube', 'https://www.youtube.com/@hoodlumortega'),
                    _socialButton(context, Icons.music_note, 'Spotify', 'https://open.spotify.com/artist/6pQmnCaVPjfQvaPUcQrhEX'),
                    _socialButton(context, Icons.music_video, 'TikTok', 'https://www.tiktok.com/@hoodlum_ortega'),
                    _socialButton(context, Icons.camera_alt, 'Instagram', 'https://www.instagram.com/hxxdlumxg'),
                    const SizedBox(height: 24),
                  ],
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _socialButton(BuildContext context, IconData icon, String label, String url) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 24, vertical: 6),
      child: SizedBox(
        width: double.infinity,
        child: OutlinedButton.icon(
          icon: Icon(icon, color: AppTheme.primaryColor),
          label: Text(label, style: const TextStyle(color: Colors.white)),
          style: OutlinedButton.styleFrom(
            side: const BorderSide(color: AppTheme.primaryColor),
            padding: const EdgeInsets.symmetric(vertical: 14),
          ),
          onPressed: () => _open(url),
        ),
      ),
    );
  }
}
