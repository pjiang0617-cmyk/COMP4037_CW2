import pandas as pd
import os
import re
import numpy as np
import plotly.graph_objects as go

CHAPTER_MAP = {
    'A': 'A: Infectious', 'B': 'B: Infectious', 'C': 'C: Neoplasms', 'D': 'D: Blood',
    'E': 'E: Endocrine', 'F': 'F: Mental', 'G': 'G: Nervous', 'H': 'H: Eye/Ear',
    'I': 'I: Circulatory', 'J': 'J: Respiratory', 'K': 'K: Digestive', 'L': 'L: Skin',
    'M': 'M: Musculoskeletal', 'N': 'N: Genitourinary', 'O': 'O: Pregnancy',
    'P': 'P: Perinatal', 'Q': 'Q: Congenital', 'R': 'R: Abnormal',
    'S': 'S: Injury', 'T': 'T: Injury', 'U': 'U: Special (COVID)',
    'V': 'V: External', 'W': 'W: External', 'X': 'X: External',
    'Y': 'Y: External', 'Z': 'Z: Health Factors'
}

def clean_and_prepare_data(df, filename):
    df.columns = [str(c).strip() for c in df.columns]

    code_col_idx = -1
    for i in range(min(5, len(df.columns))):
        sample = df.iloc[:, i].astype(str).str.strip()
        if sample.str.contains(r'^[A-Z]\d{2}', na=False).any():
            code_col_idx = i
            break

    if code_col_idx == -1: return pd.DataFrame()

    code_series = df.iloc[:, code_col_idx].astype(str).str.strip()
    mask = code_series.str.contains(r'^[A-Z]\d{2}', na=False) & \
           ~code_series.str.contains('Total|All|Grand|Summary', case=False, na=False)

    valid_rows = df[mask].copy()
    if valid_rows.empty: return pd.DataFrame()

    def parse_code_and_desc(row):
        val = str(row.iloc[code_col_idx]).strip()
        match = re.search(r'^([A-Z]\d{2})\s*(.*)', val)
        if match:
            code = match.group(1)
            desc = match.group(2).strip()
            if (not desc or len(desc) < 2) and (code_col_idx + 1 < len(row)):
                next_val = str(row.iloc[code_col_idx + 1]).strip()
                if next_val.lower() != 'nan':
                    desc = next_val
            return code, (desc if desc else "Unknown Diagnosis")
        return None, None

    parsed = valid_rows.apply(parse_code_and_desc, axis=1)

    res = pd.DataFrame()
    res['3_char'] = [p[0] for p in parsed]
    res['Description'] = [p[1] for p in parsed]
    res['Chapter_Name'] = res['3_char'].str[0].map(CHAPTER_MAP).fillna(res['3_char'].str[0])
    res['Full_Label'] = "<b>" + res['3_char'] + "</b><br>" + res['Description']

    dim_map = {
        'Total Admissions': ['admissions', 'total', 'all ages', 'fce'],
        'Children (0-14)': ['0-14', 'under 15', 'age 0', 'age 1-4', 'age 5-9', 'age 10-14'],
        'Youth (15-24)': ['15-24', 'age 15', 'age 16', 'age 17', 'age 18', 'age 19', 'age 20-24'],
        'Adults (25-64)': ['25-64', 'age 25-29', 'age 30-34', 'age 35-39', 'age 40-44', 'age 45-64'],
        'Seniors (65+)': ['65+', '65 and over', 'age 65-69', 'age 70-74', 'age 75+']
    }

    found_any = False
    for dim, keys in dim_map.items():
        matching_cols = [col for col in valid_rows.columns if any(k in col.lower() for k in keys)]
        if matching_cols:
            if dim == 'Total Admissions' and len(matching_cols) > 1:
                pref = [c for c in matching_cols if 'admissions' in c.lower() or 'total' in c.lower()]
                matching_cols = [pref[0]] if pref else [matching_cols[0]]

            val_sum = pd.Series(0.0, index=valid_rows.index)
            for col in matching_cols:
                s_val = valid_rows[col].astype(str).str.replace(',', '').replace(r'\*', '2', regex=True)
                s_val = s_val.str.replace(r'[^\d.]', '', regex=True)
                val_sum += pd.to_numeric(s_val, errors='coerce').fillna(0)
            res[dim] = val_sum.values
            if dim == 'Total Admissions': found_any = True

    match = re.search(r'20(\d{2})-(\d{2})', filename)
    if match:
        res['Year_Group'] = "Before 2019" if int(match.group(2)) <= 19 else "After 2021"
    else:
        return pd.DataFrame()

    return res

def main():
    root_dir = "./NHS Hospital Admissions"
    all_dfs = []
    if not os.path.exists(root_dir): return

    files = sorted([f for f in os.listdir(root_dir) if f.endswith('.xlsx') and not f.startswith('~$')])

    for f in files:
        try:
            with pd.ExcelFile(os.path.join(root_dir, f), engine='openpyxl') as xl:
                target = next((s for s in xl.sheet_names if '3 Char' in s), xl.sheet_names[0])
                preview = pd.read_excel(xl, sheet_name=target, nrows=50, header=None)
                h_idx = 0
                for i, row in preview.iterrows():
                    row_content = "".join(row.astype(str).str.lower()).replace(" ", "")
                    if any(k in row_content for k in ['admissions', 'fce', 'alldiagnoses']):
                        h_idx = i
                        break
                df_raw = pd.read_excel(xl, sheet_name=target, skiprows=h_idx)
                cdf = clean_and_prepare_data(df_raw, f)
                if not cdf.empty: all_dfs.append(cdf)
        except Exception as e:
            print(f"Error {f}: {e}")

    full_data = pd.concat(all_dfs, ignore_index=True)

    fig = go.Figure()
    metrics = ['Total Admissions', 'Children (0-14)', 'Youth (15-24)', 'Adults (25-64)', 'Seniors (65+)']
    periods = ["Before 2019", "After 2021"]
    trace_info = []

    for m in metrics:
        for p in periods:
            sub = full_data[full_data['Year_Group'] == p].copy()
            if sub.empty or m not in sub.columns: continue

            agg = sub.groupby(['Chapter_Name', 'Full_Label'])[m].sum().reset_index()
            agg = agg[agg[m] > 50]

            df_p = agg.groupby('Chapter_Name')[m].sum().reset_index()
            ids = df_p['Chapter_Name'].tolist() + (agg['Chapter_Name'] + "_" + agg['Full_Label']).tolist()
            labels = df_p['Chapter_Name'].tolist() + agg['Full_Label'].tolist()
            parents = [""] * len(df_p) + agg['Chapter_Name'].tolist()
            values = df_p[m].tolist() + agg[m].tolist()

            fig.add_trace(go.Treemap(
                ids=ids, labels=labels, parents=parents, values=values,
                name=f"{p}|{m}", visible=False,
                branchvalues="total",
                marker=dict(colorscale='Viridis', line=dict(width=0.5, color='white')),
                hovertemplate='<b>%{label}</b><br>Count: %{value:,.0f}<extra></extra>'
            ))
            trace_info.append({'p': p, 'm': m})

    fig.data[0].visible = True

    fig.update_layout(
        height=900,
        margin=dict(t=160, l=20, r=20, b=20),
        title={
            'text': "<b>NHS Hospital Admission Trends (ICD-10 3-Character Analysis)</b><br>" +
                    "<span style='font-size:14px; color:gray'>Robust Data Evidence: Automatically aligning diagnosis descriptions from 2012 to 2023 across inconsistent formats.</span>",
            'y': 0.94,
            'x': 0.5,
            'xanchor': 'center'
        },
        updatemenus=[
            dict(
                buttons=[dict(method="update", label=f"Period: {p}", args=[
                    {"visible": [info['p'] == p and info['m'] == trace_info[0]['m'] for info in trace_info]}]) for p in
                         periods],
                x=0.01, y=1.1, xanchor="left", yanchor="top"
            ),
            dict(
                buttons=[dict(method="update", label=f"Age Group: {m}", args=[
                    {"visible": [info['m'] == m and info['p'] == trace_info[0]['p'] for info in trace_info]}]) for m in
                         metrics],
                x=0.99, y=1.1, xanchor="right", yanchor="top"
            )
        ]
    )

    fig.update_traces(textinfo="label+value", texttemplate="%{label}<br><b>%{value:,.0s}</b>")
    fig.show()
    fig.write_html("COMP4037_CW2_Interactive_Visualization.html")

if __name__ == "__main__":
    main()