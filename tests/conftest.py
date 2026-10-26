import sys
from pathlib import Path

# permite "import code_builder" etc. sem instalar o projeto
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
