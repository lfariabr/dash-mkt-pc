import sys
from pathlib import Path
# Always add project root to sys.path for all test discovery and imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
