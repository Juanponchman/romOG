import 'ownership_status.dart';

/// ROM file model
class RomModel {
  final String filename;
  final String size;
  final bool hasAchievements;
  final OwnershipStatus ownershipStatus;
  bool isSelected;

  RomModel({
    required this.filename,
    this.size = 'N/A',
    this.hasAchievements = false,
    this.ownershipStatus = OwnershipStatus.notOwned,
    this.isSelected = false,
  });

  /// Last path segment only, for display. Handles archive.org nested paths.
  String get displayName {
    final name = filename.split('/').last;
    return name.startsWith('[RA] ') ? name.substring(5) : name;
  }

  /// Extract clean game title from filename (full name without extension)
  String get title {
    String name = displayName;
    final lastDot = name.lastIndexOf('.');
    if (lastDot > 0) {
      name = name.substring(0, lastDot);
    }
    return name;
  }

  /// Extract base title (text before first parenthesis)
  String get baseTitle {
    final name = displayName;
    final parenIndex = name.indexOf('(');
    if (parenIndex > 0) {
      return name.substring(0, parenIndex).trim();
    }
    return title;
  }

  /// Get region from filename (Europe, USA, Japan, etc.)
  String? get region {
    final name = filename;
    if (name.contains(' (USA)') || name.contains('(USA)')) return 'USA';
    if (name.contains(' (Europe)') || name.contains('(Europe)')) return 'EUR';
    if (name.contains(' (Japan)') || name.contains('(Japan)')) return 'JPN';
    return null;
  }

  factory RomModel.fromJson(Map<String, dynamic> json) {
    return RomModel(
      filename: json['filename'] ?? '',
      size: json['size'] ?? 'N/A',
      hasAchievements: json['has_achievements'] ?? false,
    );
  }

  RomModel copyWith({
    String? filename,
    String? size,
    bool? hasAchievements,
    OwnershipStatus? ownershipStatus,
    bool? isSelected,
  }) {
    return RomModel(
      filename: filename ?? this.filename,
      size: size ?? this.size,
      hasAchievements: hasAchievements ?? this.hasAchievements,
      ownershipStatus: ownershipStatus ?? this.ownershipStatus,
      isSelected: isSelected ?? this.isSelected,
    );
  }
}
