"""
ml_engine.py  —  Machine Learning engine for EWB Portfolio Monitor
Models:
  1. PTP Kept predictor       — will the client honor their promise?
  2. Payment likelihood       — probability of paying this month
  3. Best contact time        — best day-of-week & hour to call
  4. Account risk score       — high / medium / low priority
"""

import os
import numpy as np
import pandas as pd
import joblib
from datetime import date, datetime
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.calibration import CalibratedClassifierCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "models")
os.makedirs(MODEL_DIR, exist_ok=True)

TODAY = date.today()

# ─────────────────────────────────────────────────────────────────────────────
# FEATURE ENGINEERING HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _safe_days(d1, d2=None):
    """Days between two dates. Returns 0 on error."""
    try:
        d2 = d2 or pd.Timestamp(TODAY)
        return max(0, (pd.Timestamp(d2) - pd.Timestamp(d1)).days)
    except Exception:
        return 0


def _ob_bucket(ob):
    """Bucket OB into 5 bands."""
    try:
        ob = float(ob)
        if ob < 10_000:    return 0
        if ob < 50_000:    return 1
        if ob < 100_000:   return 2
        if ob < 500_000:   return 3
        return 4
    except Exception:
        return 0


def _encode_col(series: pd.Series) -> np.ndarray:
    le = LabelEncoder()
    filled = series.fillna("UNKNOWN").astype(str)
    return le.fit_transform(filled)


def _build_recovery_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a feature matrix from EWB_RECOVERY data.
    One row per unique ACCT_NO (latest status used).
    """
    df = df.copy()
    df["BARCODE_DATE"] = pd.to_datetime(df["BARCODE_DATE"], errors="coerce")
    df["ENDO_DATE"]    = pd.to_datetime(df["ENDO_DATE"],    errors="coerce")
    df["AMOUNT"]       = pd.to_numeric(df["AMOUNT"],        errors="coerce").fillna(0)
    df["OB"]           = pd.to_numeric(df["OB"],            errors="coerce").fillna(0)

    # Per-account aggregates
    agg = df.groupby("ACCT_NO").agg(
        total_efforts     = ("RESULT_ID",     "count"),
        ptp_count         = ("STATUS",        lambda s: s.str.contains("PTP", case=False, na=False).sum()),
        payment_count     = ("STATUS",        lambda s: s.str.contains("PAYMENT|PAID|COLL", case=False, na=False).sum()),
        max_amount        = ("AMOUNT",        "max"),
        sum_amount        = ("AMOUNT",        "sum"),
        ob                = ("OB",            "first"),
        latest_barcode    = ("BARCODE_DATE",  "max"),
        earliest_barcode  = ("BARCODE_DATE",  "min"),
        latest_status     = ("STATUS",        "last"),
        latest_substatus  = ("SUB_STATUS",    "last"),
        latest_agent      = ("AGENT",         "last"),
        placement         = ("PLACEMENT",     "first"),
    ).reset_index()

    agg["days_since_last_touch"] = agg["latest_barcode"].apply(
        lambda d: _safe_days(d) if pd.notna(d) else 999
    )
    agg["days_in_portfolio"] = agg["earliest_barcode"].apply(
        lambda d: _safe_days(d) if pd.notna(d) else 0
    )
    agg["ptp_rate"]     = (agg["ptp_count"]     / agg["total_efforts"].clip(lower=1))
    agg["payment_rate"] = (agg["payment_count"] / agg["total_efforts"].clip(lower=1))
    agg["ob_bucket"]    = agg["ob"].apply(_ob_bucket)
    agg["status_enc"]   = _encode_col(agg["latest_status"])
    agg["placement_enc"]= _encode_col(agg["placement"])
    agg["agent_enc"]    = _encode_col(agg["latest_agent"])

    # Best contact day (mode of barcode_date weekday)
    weekday_mode = (
        df.dropna(subset=["BARCODE_DATE"])
        .groupby("ACCT_NO")["BARCODE_DATE"]
        .apply(lambda s: s.dt.dayofweek.mode()[0] if len(s) else 0)
        .reset_index(name="best_contact_day")
    )
    agg = agg.merge(weekday_mode, on="ACCT_NO", how="left")
    agg["best_contact_day"] = agg["best_contact_day"].fillna(0).astype(int)

    return agg


def _feature_cols():
    return [
        "total_efforts", "ptp_count", "payment_count",
        "max_amount", "sum_amount", "ob_bucket",
        "days_since_last_touch", "days_in_portfolio",
        "ptp_rate", "payment_rate",
        "status_enc", "placement_enc", "agent_enc",
        "best_contact_day",
    ]


# ─────────────────────────────────────────────────────────────────────────────
# MODEL 1 — PTP KEPT PREDICTOR
# ─────────────────────────────────────────────────────────────────────────────

def train_ptp_model(df_recovery: pd.DataFrame) -> dict:
    """
    Label: account had at least one PTP that was followed by a PAYMENT
    (same ACCT_NO, payment barcode_date >= PTP barcode_date).
    """
    df = df_recovery.copy()
    df["BARCODE_DATE"] = pd.to_datetime(df["BARCODE_DATE"], errors="coerce")
    df["AMOUNT"]       = pd.to_numeric(df["AMOUNT"], errors="coerce").fillna(0)

    ptp_accts = set(df[df["STATUS"].str.contains("PTP", case=False, na=False)]["ACCT_NO"])
    pay_accts = set(df[df["STATUS"].str.contains("PAYMENT|PAID|COLL", case=False, na=False)]["ACCT_NO"])
    kept_accts = ptp_accts & pay_accts

    feats = _build_recovery_features(df)
    feats = feats[feats["ACCT_NO"].isin(ptp_accts)].copy()
    if len(feats) < 20:
        return {"status": "insufficient_data", "min_required": 20, "available": len(feats)}

    feats["label"] = feats["ACCT_NO"].isin(kept_accts).astype(int)
    X = feats[_feature_cols()].fillna(0)
    y = feats["label"]

    if y.nunique() < 2:
        return {"status": "insufficient_data", "reason": "only one class present"}

    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    clf = CalibratedClassifierCV(
        RandomForestClassifier(n_estimators=100, random_state=42, class_weight="balanced"),
        cv=3
    )
    clf.fit(X_tr, y_tr)
    acc = clf.score(X_te, y_te)

    path = os.path.join(MODEL_DIR, "ptp_model.pkl")
    joblib.dump(clf, path)
    return {"status": "trained", "accuracy": round(acc * 100, 1), "samples": len(feats)}


def predict_ptp_kept(df_recovery: pd.DataFrame) -> pd.DataFrame:
    """Return per-account PTP-kept probability."""
    path = os.path.join(MODEL_DIR, "ptp_model.pkl")
    feats = _build_recovery_features(df_recovery)
    X = feats[_feature_cols()].fillna(0)

    if os.path.exists(path):
        clf = joblib.load(path)
    else:
        info = train_ptp_model(df_recovery)
        if info.get("status") != "trained":
            feats["ptp_kept_prob"] = 0.5
            feats["ptp_kept_label"] = "Unknown"
            return feats[["ACCT_NO", "ptp_kept_prob", "ptp_kept_label"]]
        clf = joblib.load(path)

    probs = clf.predict_proba(X)[:, 1]
    feats["ptp_kept_prob"]  = (probs * 100).round(1)
    feats["ptp_kept_label"] = pd.cut(
        probs, bins=[0, 0.35, 0.65, 1.0],
        labels=["🔴 Likely Broken", "🟡 Uncertain", "🟢 Likely Kept"]
    )
    return feats[["ACCT_NO", "ptp_kept_prob", "ptp_kept_label",
                  "latest_agent", "placement", "latest_status",
                  "ob", "ptp_count", "payment_count", "days_since_last_touch"]]


# ─────────────────────────────────────────────────────────────────────────────
# MODEL 2 — PAYMENT LIKELIHOOD
# ─────────────────────────────────────────────────────────────────────────────

def train_payment_model(df_recovery: pd.DataFrame) -> dict:
    """Label: account had a payment posted this month."""
    df = df_recovery.copy()
    df["BARCODE_DATE"] = pd.to_datetime(df["BARCODE_DATE"], errors="coerce")

    paid_this_month = set(df[
        df["STATUS"].str.contains("PAYMENT|PAID|COLL", case=False, na=False) &
        (df["BARCODE_DATE"].dt.year  == TODAY.year) &
        (df["BARCODE_DATE"].dt.month == TODAY.month)
    ]["ACCT_NO"])

    feats = _build_recovery_features(df)
    if len(feats) < 20:
        return {"status": "insufficient_data", "available": len(feats)}

    feats["label"] = feats["ACCT_NO"].isin(paid_this_month).astype(int)
    X = feats[_feature_cols()].fillna(0)
    y = feats["label"]

    if y.nunique() < 2:
        return {"status": "insufficient_data", "reason": "only one class present"}

    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    clf = CalibratedClassifierCV(
        GradientBoostingClassifier(n_estimators=100, random_state=42),
        cv=3
    )
    clf.fit(X_tr, y_tr)
    acc = clf.score(X_te, y_te)

    path = os.path.join(MODEL_DIR, "payment_model.pkl")
    joblib.dump(clf, path)
    return {"status": "trained", "accuracy": round(acc * 100, 1), "samples": len(feats)}


def predict_payment_likelihood(df_recovery: pd.DataFrame) -> pd.DataFrame:
    path = os.path.join(MODEL_DIR, "payment_model.pkl")
    feats = _build_recovery_features(df_recovery)
    X = feats[_feature_cols()].fillna(0)

    if os.path.exists(path):
        clf = joblib.load(path)
    else:
        info = train_payment_model(df_recovery)
        if info.get("status") != "trained":
            feats["pay_prob"] = 0.5
            feats["pay_label"] = "Unknown"
            return feats[["ACCT_NO", "pay_prob", "pay_label"]]
        clf = joblib.load(path)

    probs = clf.predict_proba(X)[:, 1]
    feats["pay_prob"]  = (probs * 100).round(1)
    feats["pay_label"] = pd.cut(
        probs, bins=[0, 0.30, 0.60, 1.0],
        labels=["🔴 Low", "🟡 Medium", "🟢 High"]
    )
    return feats[["ACCT_NO", "pay_prob", "pay_label",
                  "latest_agent", "placement", "ob",
                  "ptp_count", "payment_count", "days_since_last_touch"]]


# ─────────────────────────────────────────────────────────────────────────────
# MODEL 3 — BEST CONTACT TIME (no model needed, rule-based from history)
# ─────────────────────────────────────────────────────────────────────────────

DAYS = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

def best_contact_analysis(df_recovery: pd.DataFrame) -> dict:
    """
    Analyse which day-of-week has the highest PTP + Payment rate.
    Returns a summary dict with charts-ready data.
    """
    df = df_recovery.copy()
    df["BARCODE_DATE"] = pd.to_datetime(df["BARCODE_DATE"], errors="coerce")
    df = df.dropna(subset=["BARCODE_DATE"])
    df["DOW"]  = df["BARCODE_DATE"].dt.dayofweek      # 0=Mon
    df["WEEK"] = df["BARCODE_DATE"].dt.isocalendar().week.astype(int)

    df["is_ptp"]     = df["STATUS"].str.contains("PTP",              case=False, na=False)
    df["is_payment"] = df["STATUS"].str.contains("PAYMENT|PAID|COLL", case=False, na=False)

    dow_stats = df.groupby("DOW").agg(
        Efforts  = ("RESULT_ID",   "count"),
        PTPs     = ("is_ptp",      "sum"),
        Payments = ("is_payment",  "sum"),
    ).reset_index()
    dow_stats["Day"]         = dow_stats["DOW"].apply(lambda d: DAYS[d])
    dow_stats["PTP Rate"]    = (dow_stats["PTPs"]     / dow_stats["Efforts"].clip(lower=1) * 100).round(1)
    dow_stats["Pay Rate"]    = (dow_stats["Payments"] / dow_stats["Efforts"].clip(lower=1) * 100).round(1)
    dow_stats["Score"]       = (dow_stats["PTP Rate"] + dow_stats["Pay Rate"]).round(1)
    dow_stats = dow_stats.sort_values("DOW")

    best_day_idx = dow_stats["Score"].idxmax()
    best_day     = dow_stats.loc[best_day_idx, "Day"]

    return {
        "dow_stats": dow_stats[["Day","Efforts","PTPs","Payments","PTP Rate","Pay Rate","Score"]],
        "best_day":  best_day,
    }


# ─────────────────────────────────────────────────────────────────────────────
# MODEL 4 — ACCOUNT RISK SCORING
# ─────────────────────────────────────────────────────────────────────────────

def compute_risk_scores(df_recovery: pd.DataFrame) -> pd.DataFrame:
    """
    Rule-based + weighted risk score per account (0–100).
    Higher score = higher priority to contact.
    """
    feats = _build_recovery_features(df_recovery)

    # Weighted score components (all 0-1 normalised)
    max_ob  = feats["ob"].clip(lower=0).max() or 1
    max_eff = feats["total_efforts"].clip(lower=0).max() or 1

    score = (
        0.35 * (feats["ob"].clip(lower=0) / max_ob)                     +  # high OB = high priority
        0.25 * feats["ptp_rate"]                                          +  # made promises = likely to pay
        0.20 * (1 - (feats["days_since_last_touch"].clip(upper=90) / 90)) +  # recently touched
        0.10 * feats["payment_rate"]                                       +  # history of paying
        0.10 * (feats["total_efforts"].clip(lower=0) / max_eff)             # effort invested
    )

    feats["risk_score"] = (score * 100).round(1)
    feats["risk_label"] = pd.cut(
        score, bins=[0, 0.33, 0.66, 1.01],
        labels=["🔵 Low Priority", "🟡 Medium Priority", "🔴 High Priority"]
    )

    return feats[[
        "ACCT_NO", "risk_score", "risk_label",
        "latest_agent", "placement", "ob",
        "ptp_count", "payment_count",
        "days_since_last_touch", "total_efforts",
        "latest_status",
    ]].sort_values("risk_score", ascending=False)


# ─────────────────────────────────────────────────────────────────────────────
# TRAIN ALL MODELS AT ONCE
# ─────────────────────────────────────────────────────────────────────────────

def train_all_models(df_recovery: pd.DataFrame) -> dict:
    results = {}
    results["ptp"]     = train_ptp_model(df_recovery)
    results["payment"] = train_payment_model(df_recovery)
    return results
