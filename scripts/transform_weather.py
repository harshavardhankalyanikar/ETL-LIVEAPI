
import json
from pathlib import Path
import pandas as pd


def transform_weather_data(city: str = "Hyderabad") -> pd.DataFrame:
    repo_root = Path(__file__).resolve().parent.parent
    raw_dir = repo_root / "data" / "raw"
    staged_dir = repo_root / "data" / "staged"
    staged_dir.mkdir(parents=True, exist_ok=True)

    json_files = list(raw_dir.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {raw_dir}")

    latest_file = max(json_files, key=lambda p: p.stat().st_mtime)

    with latest_file.open("r", encoding="utf-8") as f:
        data = json.load(f)

    hourly = data.get("hourly")
    if hourly is None:
        raise KeyError("'hourly' key not found in JSON file")

    # Map fields from the source JSON to clearer column names
    df = pd.DataFrame(
        {
            "time": hourly.get("time", []),
            "temperature_C": hourly.get("temperature_2m"),
            "relative_humidity_pct": hourly.get("relative_humidity_2m"),
            "wind_speed_kmph": hourly.get("wind_speed_10m"),
        }
    )

    # Normalize and enrich
    if not df.empty:
        df["time"] = pd.to_datetime(df["time"])
    df["city"] = city
    df["extracted_at"] = pd.Timestamp.now()

    output_path = staged_dir / "weather_cleaned.csv"
    df.to_csv(output_path, index=False)

    print(f"Transformed {len(df)} records from '{latest_file.name}' -> {output_path}")
    return df


if __name__ == "__main__":
    transform_weather_data()