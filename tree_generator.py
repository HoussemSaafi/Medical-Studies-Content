#!/usr/bin/env python3
"""
Folder Tree Generator Script
Generates a tree-like view of folder and file structure from a base directory.
Supports filtering, size information, and various output formats.

Usage:
    python tree_generator.py [path] [options]
    
Examples:
    python tree_generator.py                    # Current directory
    python tree_generator.py /path/to/folder    # Specific path
    python tree_generator.py . --max-depth 3    # Limit depth
    python tree_generator.py . --ignore-hidden  # Skip hidden files
    python tree_generator.py . --save tree.txt  # Save to file
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

class TreeGenerator:
    def __init__(self, show_hidden=True, show_size=False, max_depth=None, ignore_patterns=None):
        self.show_hidden = show_hidden
        self.show_size = show_size
        self.max_depth = max_depth
        self.ignore_patterns = ignore_patterns or []
        self.file_count = 0
        self.folder_count = 0
        
        # Tree drawing characters
        self.PIPE = "│"
        self.TEE = "├"
        self.LAST = "└"
        self.PIPE_PREFIX = "│   "
        self.TEE_PREFIX = "├── "
        self.LAST_PREFIX = "└── "
        self.SPACE_PREFIX = "    "
    
    def should_ignore(self, path):
        """Check if path should be ignored based on patterns"""
        path_str = str(path)
        name = path.name
        
        # Always skip .keep files
        if name == '.keep':
            return True
        
        # Skip hidden files if requested
        if not self.show_hidden and name.startswith('.'):
            return True
        
        # Check ignore patterns
        for pattern in self.ignore_patterns:
            if pattern in path_str or pattern in name:
                return True
        
        return False
    
    def is_empty_folder(self, directory):
        """
        Check if directory is empty or contains only .keep files
        """
        try:
            items = list(directory.iterdir())
            
            # No items at all
            if not items:
                return True
            
            # Filter out ignored items (including .keep files)
            visible_items = [item for item in items if not self.should_ignore(item)]
            
            # If no visible items remain, folder is considered empty
            if not visible_items:
                return True
            
            # Check if all remaining items are empty directories
            all_empty_dirs = True
            for item in visible_items:
                if item.is_file():
                    all_empty_dirs = False
                    break
                elif item.is_dir() and not self.is_empty_folder(item):
                    all_empty_dirs = False
                    break
            
            return all_empty_dirs
            
        except (OSError, PermissionError):
            return False
        """Convert bytes to human-readable format"""
        if size_bytes < 1024:
            return f"{size_bytes}B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes/1024:.1f}KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes/(1024*1024):.1f}MB"
        else:
            return f"{size_bytes/(1024*1024*1024):.1f}GB"
    
    def get_file_info(self, path):
        """Get file size and other info"""
        try:
            stat = path.stat()
            size = stat.st_size
            if self.show_size:
                return f" ({self.format_size(size)})"
            return ""
        except (OSError, PermissionError):
            return " (no access)"
    
    def generate_tree(self, root_path, output_file=None):
        """Generate the tree structure"""
        root = Path(root_path).resolve()
        
        if not root.exists():
            print(f"Error: Path '{root_path}' does not exist")
            return
        
        if not root.is_dir():
            print(f"Error: '{root_path}' is not a directory")
            return
        
        # Prepare output
        output_lines = []
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Header
        header = f"""
📁 FOLDER TREE STRUCTURE
{'='*50}
📍 Root: {root}
🕐 Generated: {timestamp}
{'='*50}
"""
        output_lines.append(header)
        
        # Generate tree
        output_lines.append(f"📁 {root.name}/")
        self._generate_tree_recursive(root, "", True, output_lines, 0)
        
        # Footer with statistics
        footer = f"""
{'='*50}
📊 SUMMARY:
   📁 Folders: {self.folder_count}
   📄 Files: {self.file_count}
   📊 Total items: {self.folder_count + self.file_count}
{'='*50}
"""
        output_lines.append(footer)
        
        # Output results
        tree_content = "\n".join(output_lines)
        
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(tree_content)
                print(f"✅ Tree structure saved to: {output_file}")
                print(f"📊 Found {self.folder_count} folders and {self.file_count} files")
            except Exception as e:
                print(f"❌ Error saving to file: {e}")
                print(tree_content)
        else:
            print(tree_content)
    
    def _generate_tree_recursive(self, directory, prefix, is_last, output_lines, depth):
        """Recursively generate tree structure"""
        # Check depth limit
        if self.max_depth is not None and depth >= self.max_depth:
            return
        
        try:
            # Get all items in directory
            items = list(directory.iterdir())
            
            # Filter items (this will remove .keep files and other ignored items)
            items = [item for item in items if not self.should_ignore(item)]
            
            # Remove empty folders (folders that contain only .keep files or are truly empty)
            filtered_items = []
            for item in items:
                if item.is_file():
                    # Keep all files that aren't ignored
                    filtered_items.append(item)
                elif item.is_dir() and not self.is_empty_folder(item):
                    # Keep directories that aren't empty (don't contain only .keep files)
                    filtered_items.append(item)
                # Skip empty directories and directories with only .keep files
            
            items = filtered_items
            
            # Sort: directories first, then files, both alphabetically
            items.sort(key=lambda x: (x.is_file(), x.name.lower()))
            
            for i, item in enumerate(items):
                is_last_item = (i == len(items) - 1)
                
                # Choose prefix for this item
                if is_last_item:
                    current_prefix = self.LAST_PREFIX
                    next_prefix = prefix + self.SPACE_PREFIX
                else:
                    current_prefix = self.TEE_PREFIX
                    next_prefix = prefix + self.PIPE_PREFIX
                
                # Format item name
                if item.is_dir():
                    self.folder_count += 1
                    icon = "📁"
                    name = f"{item.name}/"
                    file_info = ""
                else:
                    self.file_count += 1
                    icon = self._get_file_icon(item)
                    name = item.name
                    file_info = self.get_file_info(item)
                
                # Add to output
                line = f"{prefix}{current_prefix}{icon} {name}{file_info}"
                output_lines.append(line)
                
                # Recurse into directories
                if item.is_dir():
                    self._generate_tree_recursive(
                        item, next_prefix, is_last_item, output_lines, depth + 1
                    )
                    
        except PermissionError:
            line = f"{prefix}{self.TEE_PREFIX}❌ Permission denied"
            output_lines.append(line)
    
    def _get_file_icon(self, file_path):
        """Get appropriate icon for file type"""
        suffix = file_path.suffix.lower()
        
        icons = {
            # Documents
            '.pdf': '📄',
            '.doc': '📄', '.docx': '📄',
            '.txt': '📄', '.md': '📄', '.rst': '📄',
            '.rtf': '📄',
            
            # Spreadsheets
            '.xls': '📊', '.xlsx': '📊', '.csv': '📊',
            
            # Presentations
            '.ppt': '📊', '.pptx': '📊',
            
            # Images
            '.jpg': '🖼️', '.jpeg': '🖼️', '.png': '🖼️',
            '.gif': '🖼️', '.bmp': '🖼️', '.svg': '🖼️',
            '.ico': '🖼️', '.tiff': '🖼️',
            
            # Audio
            '.mp3': '🎵', '.wav': '🎵', '.flac': '🎵',
            '.aac': '🎵', '.ogg': '🎵', '.m4a': '🎵',
            
            # Video
            '.mp4': '🎬', '.avi': '🎬', '.mkv': '🎬',
            '.mov': '🎬', '.wmv': '🎬', '.flv': '🎬',
            
            # Code files
            '.py': '🐍', '.js': '🟨', '.html': '🌐',
            '.css': '🎨', '.json': '📋', '.xml': '📋',
            '.sql': '🗃️', '.sh': '⚡', '.bat': '⚡',
            '.java': '☕', '.cpp': '⚙️', '.c': '⚙️',
            '.php': '🐘', '.rb': '💎', '.go': '🐹',
            '.rs': '🦀', '.swift': '🦉', '.kt': '🎯',
            
            # Archives
            '.zip': '📦', '.rar': '📦', '.7z': '📦',
            '.tar': '📦', '.gz': '📦', '.bz2': '📦',
            
            # Config files
            '.ini': '⚙️', '.cfg': '⚙️', '.conf': '⚙️',
            '.yml': '⚙️', '.yaml': '⚙️', '.toml': '⚙️',
            
            # Executables
            '.exe': '⚡', '.app': '⚡', '.deb': '📦',
            '.rpm': '📦', '.msi': '📦',
        }
        
        return icons.get(suffix, '📄')

def main():
    parser = argparse.ArgumentParser(
        description="Generate a tree view of folder structure",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                           # Current directory
  %(prog)s /path/to/folder          # Specific path
  %(prog)s . --max-depth 3          # Limit depth to 3 levels
  %(prog)s . --ignore-hidden        # Skip hidden files/folders
  %(prog)s . --show-size            # Show file sizes
  %(prog)s . --save tree.txt        # Save output to file
  %(prog)s . --ignore node_modules __pycache__  # Ignore specific folders
        """
    )
    
    parser.add_argument(
        'path',
        nargs='?',
        default='.',
        help='Path to generate tree for (default: current directory)'
    )
    
    parser.add_argument(
        '--max-depth', '-d',
        type=int,
        help='Maximum depth to traverse'
    )
    
    parser.add_argument(
        '--ignore-hidden',
        action='store_true',
        help='Ignore hidden files and folders (starting with .)'
    )
    
    parser.add_argument(
        '--show-size', '-s',
        action='store_true',
        help='Show file sizes'
    )
    
    parser.add_argument(
        '--save',
        help='Save output to specified file'
    )
    
    parser.add_argument(
        '--ignore',
        nargs='*',
        default=[],
        help='Patterns to ignore (folder/file names containing these strings)'
    )
    
    args = parser.parse_args()
    
    # Add common ignore patterns for development and empty folders
    common_ignores = ['__pycache__', '.git', 'node_modules', '.DS_Store', 'Thumbs.db', '.keep']
    ignore_patterns = list(set(args.ignore + common_ignores))
    
    # Create tree generator
    generator = TreeGenerator(
        show_hidden=not args.ignore_hidden,
        show_size=args.show_size,
        max_depth=args.max_depth,
        ignore_patterns=ignore_patterns
    )
    
    # Generate tree
    generator.generate_tree(args.path, args.save)

if __name__ == "__main__":
    main()
