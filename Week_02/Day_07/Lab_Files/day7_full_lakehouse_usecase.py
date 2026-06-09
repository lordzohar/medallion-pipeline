from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "labs"))

from labs.lab_12_full_medallion_pipeline import main


if __name__ == "__main__":
    main()
