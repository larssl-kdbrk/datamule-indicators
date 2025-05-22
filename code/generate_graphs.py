import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from aquarel import load_theme
import math # For ceiling function for subplot rows

# Apply Aquarel theme
theme = load_theme("scientific")
theme.apply()

# Define paths
INDICATORS_JSON_PATH = "indicators/indicators.json"
INDICATORS_BASE_PATH = "indicators/format1"
GRAPHS_OUTPUT_DIR = "graphs"
DATA_FILENAME = "overview.csv" # Standard filename for data

# Create graphs output directory if it doesn't exist
os.makedirs(GRAPHS_OUTPUT_DIR, exist_ok=True)

# Load indicators
with open(INDICATORS_JSON_PATH, 'r') as f:
    indicators_data = json.load(f)

# Iterate through categories and indicators
for category, indicators in indicators_data["categories"].items():
    if category == "Other": # Skip "Other" category as it's empty
        continue
    for indicator_name in indicators:
        csv_path = os.path.join(INDICATORS_BASE_PATH, category, indicator_name, DATA_FILENAME)
        graph_path = os.path.join(GRAPHS_OUTPUT_DIR, f"{indicator_name}.png")

        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)

                if 'filing_date' not in df.columns or 'count' not in df.columns or 'component' not in df.columns:
                    print(f"Skipping {csv_path}: 'filing_date', 'count', or 'component' column missing.")
                    continue

                df = df.rename(columns={'filing_date': 'date', 'count': 'value'})
                df['date'] = pd.to_datetime(df['date'])
                
                components = df['component'].unique()
                n_components = len(components)

                if n_components == 0:
                    print(f"Skipping {csv_path}: No components found.")
                    continue

                if n_components == 1:
                    n_rows, n_cols = 1, 1
                    fig_size = (10, 6)
                else:
                    n_cols = 2 
                    n_rows = math.ceil(n_components / n_cols)
                    fig_width = n_cols * 7 
                    fig_height = n_rows * 5 
                    fig_size = (fig_width, fig_height)

                fig, axes = plt.subplots(n_rows, n_cols, figsize=fig_size, squeeze=False) 
                
                fig.suptitle(f"{category} - {indicator_name.replace('-', ' ').title()}", fontsize=16, y=0.97) # Lowered y for suptitle

                for i, component_name in enumerate(components):
                    ax = axes[i // n_cols, i % n_cols]
                    component_df = df[df['component'] == component_name]

                    ax.plot(component_df['date'], component_df['value'], marker='o', linestyle='-', markersize=4) # Reduced markersize
                    ax.set_title(component_name.replace('_', ' ').title(), fontsize=10)
                    ax.set_xlabel("Date", fontsize=8)
                    ax.set_ylabel("Count", fontsize=8)
                    ax.grid(True)
                    ax.tick_params(axis='x', rotation=45, labelsize=7)
                    ax.tick_params(axis='y', labelsize=7)
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
                
                for j in range(n_components, n_rows * n_cols):
                    fig.delaxes(axes[j // n_cols, j % n_cols])
                
                # Adjusted tight_layout with rect and padding
                plt.tight_layout(rect=[0, 0.03, 1, 0.94], h_pad=2.0, w_pad=1.5) 
                plt.savefig(graph_path)
                plt.close(fig) 
                print(f"Generated graph: {graph_path}")

            except Exception as e:
                print(f"Error processing {csv_path}: {e}")
        else:
            print(f"CSV file not found: {csv_path}")

print("Graph generation complete.") 