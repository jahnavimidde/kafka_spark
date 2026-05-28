import streamlit as st
import pandas as pd
import os
import pydeck as pdk

st.set_page_config(layout="wide", page_title="🚦 Smart Traffic Dashboard")

# ── CONFIG ────────────────────────────────────────────────────────────────────
FOLDER       = os.environ.get("OUTPUT_DIR", "C:/traffic-project/output")
REFRESH_SECS = 10    # match producer poll interval
HISTORY_LEN  = 30

# ── COORDINATES ───────────────────────────────────────────────────────────────
COORDS = {
    "Ameerpet":     (17.4375, 78.4482),
    "Gachibowli":   (17.4401, 78.3489),
    "Kukatpally":   (17.4948, 78.3996),
    "Secunderabad": (17.4399, 78.4983),
    "Miyapur":      (17.4969, 78.3562),
    "Madhapur":     (17.4483, 78.3915),
    "BanjaraHills": (17.4126, 78.4482),
    "JubileeHills": (17.4239, 78.4738),
    "LBNagar":      (17.3457, 78.5522),
    "Dilsukhnagar": (17.3688, 78.5247),
    "Charminar":    (17.3616, 78.4747),
    "Mehdipatnam":  (17.3950, 78.4330),
    "Begumpet":     (17.4448, 78.4667),
    "HitechCity":   (17.4435, 78.3772),
    "Uppal":        (17.4050, 78.5591),
}

LEVEL_COLOR = {
    "HIGH":   [220, 50,  50],
    "MEDIUM": [255, 165,  0],
    "LOW":    [40,  180, 80],
}

# ── SESSION STATE ─────────────────────────────────────────────────────────────
if "history" not in st.session_state:
    st.session_state.history = []

# ── DATA LOADER ───────────────────────────────────────────────────────────────
def load_data(folder: str) -> pd.DataFrame | None:
    if not os.path.isdir(folder):
        return None

    frames = []
    for fname in os.listdir(folder):
        if not fname.endswith(".csv"):
            continue
        path = os.path.join(folder, fname)
        if os.path.getsize(path) == 0:
            continue
        try:
            df = pd.read_csv(path)
            # Graceful fallback for old-format CSV files
            if "location" not in df.columns:
                df = pd.read_csv(path, header=None)
                df.columns = ["location", "vehicles", "congestion_level",
                               "current_speed", "freeflow_speed",
                               "ratio", "confidence", "timestamp"][:len(df.columns)]
            frames.append(df)
        except Exception as e:
            st.warning(f"Could not read {fname}: {e}")

    if not frames:
        return None

    data = pd.concat(frames, ignore_index=True)
    data["vehicles"] = pd.to_numeric(data["vehicles"], errors="coerce")
    data["ratio"]    = pd.to_numeric(data.get("ratio"),    errors="coerce")
    data = data.dropna(subset=["vehicles"])
    data = data.groupby("location", as_index=False).last()
    data = data.sort_values("location").reset_index(drop=True)
    return data

# ── LIVE FRAGMENT (no scroll jump on refresh) ─────────────────────────────────
@st.fragment(run_every=REFRESH_SECS)
def live_dashboard():
    data = load_data(FOLDER)

    if data is None or data.empty:
        st.warning(f"⏳ Waiting for data in: {FOLDER}")
        return

    # ── TABLE ──────────────────────────────────────────────────────────────────
    st.subheader("📊 Live Traffic Data")

    # Show cleaner column names in display
    display_cols = ["location", "vehicles", "congestion_level"]
    if "current_speed" in data.columns:
        display_cols += ["current_speed", "freeflow_speed", "ratio", "confidence"]
    if "timestamp" in data.columns:
        display_cols.append("timestamp")

    display_df = data[[c for c in display_cols if c in data.columns]].copy()
    if "ratio" in display_df.columns:
        display_df["ratio"] = display_df["ratio"].apply(
            lambda x: f"{x:.2f}" if pd.notna(x) else ""
        )

    st.dataframe(display_df, use_container_width=True)

    # ── METRICS ────────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🔴 HIGH",   int((data["congestion_level"] == "HIGH").sum()))
    c2.metric("🟡 MEDIUM", int((data["congestion_level"] == "MEDIUM").sum()))
    c3.metric("🟢 LOW",    int((data["congestion_level"] == "LOW").sum()))

    if "current_speed" in data.columns:
        avg_speed = data["current_speed"].mean()
        c4.metric("⚡ Avg Speed", f"{avg_speed:.1f} km/h")

    # ── MAP ────────────────────────────────────────────────────────────────────
    map_rows = []
    for _, row in data.iterrows():
        loc = row["location"]
        if loc in COORDS:
            lat, lon = COORDS[loc]
            level = row["congestion_level"]
            # Radius scales with congestion: HIGH = bigger dot
            radius = {"HIGH": 550, "MEDIUM": 400, "LOW": 280}.get(level, 400)
            map_rows.append({
                "lat":           lat,
                "lon":           lon,
                "color":         LEVEL_COLOR.get(level, [128, 128, 128]),
                "level":         level,
                "vehicles":      int(row["vehicles"]),
                "current_speed": row.get("current_speed", "N/A"),
                "radius":        radius,
            })

    if map_rows:
        st.subheader("🌍 Traffic Map")
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=pd.DataFrame(map_rows),
            get_position="[lon, lat]",
            get_color="color",
            get_radius="radius",
            pickable=True,
        )
        st.pydeck_chart(pdk.Deck(
            layers=[layer],
            initial_view_state=pdk.ViewState(
                latitude=17.3850, longitude=78.4867, zoom=11
            ),
            tooltip={"text": "{level}\nVehicles: {vehicles}\nSpeed: {current_speed} km/h"},
        ))

    # ── SPEED TREND GRAPH ──────────────────────────────────────────────────────
    avg_vehicles = float(data["vehicles"].mean())
    point = {"time": pd.Timestamp.now(), "avg_vehicles": round(avg_vehicles, 1)}

    if "current_speed" in data.columns:
        point["avg_speed_kmh"] = round(float(data["current_speed"].mean()), 1)

    st.session_state.history.append(point)
    st.session_state.history = st.session_state.history[-HISTORY_LEN:]

    st.subheader("📈 Traffic Trend")
    history_df = pd.DataFrame(st.session_state.history).set_index("time")
    st.line_chart(history_df)


# ── STATIC HEADER (rendered once, never reruns) ───────────────────────────────
st.title("🚦 Smart Traffic Dashboard — Hyderabad (Live)")
st.caption(f"Data source: TomTom Traffic API · Refreshes every {REFRESH_SECS}s")
live_dashboard()