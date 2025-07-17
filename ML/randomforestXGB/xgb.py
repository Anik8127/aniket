import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV, TimeSeriesSplit
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier # Ensure this is installed: pip install xgboost
from sklearn.metrics import (accuracy_score, precision_score, recall_score,
                             f1_score, roc_auc_score, confusion_matrix,
                             classification_report)
import warnings

warnings.filterwarnings('ignore')

# --- 1. Data Loading and Initial Preparation ---
print("Loading and preparing data...")
df = pd.read_csv('/root/aniket/AARTIIND.csv', parse_dates=['datetime'])
df = df.sort_values('datetime').reset_index(drop=True)

# Calculate candle color (1 for green, 0 for red)
df['current_color'] = (df['c'] > df['o']).astype(int)
df['next_color'] = df['current_color'].shift(-1)  # Target variable

# Drop the last row which has no next color (as it lacks a 'next_color')
df = df[:-1]

# --- 2. Feature Engineering ---
print("Creating features...")


def create_features(df_input):
    df_output = df_input.copy()

    # Basic price features
    df_output['price_change'] = df_output['c'].pct_change()
    df_output['range'] = (df_output['h'] - df_output['l']) / df_output['c'].shift(1)
    df_output['body'] = abs(df_output['c'] - df_output['o']) / df_output['c'].shift(1)
    df_output['upper_shadow'] = (df_output['h'] - np.maximum(df_output['c'], df_output['o'])) / df_output[
        'c'].shift(1)
    df_output['lower_shadow'] = (np.minimum(df_output['c'], df_output['o']) - df_output['l']) / df_output[
        'c'].shift(1)

    # Volume features
    df_output['volume_change'] = df_output['v'].pct_change()
    df_output['volume_ma_5'] = df_output['v'].rolling(5).mean()
    df_output['volume_ma_10'] = df_output['v'].rolling(10).mean()

    # Moving averages
    df_output['ma_5'] = df_output['c'].rolling(5).mean()
    df_output['ma_10'] = df_output['c'].rolling(10).mean()
    df_output['ma_20'] = df_output['c'].rolling(20).mean()

    # Price momentum
    df_output['momentum_5'] = df_output['c'].pct_change(5)
    df_output['momentum_10'] = df_output['c'].pct_change(10)

    # RSI (Relative Strength Index)
    # Calculation adjusted to handle division by zero for initial periods or constant prices
    delta = df_output['c'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    # Handle cases where loss might be zero to prevent division by zero
    rs = np.where(loss == 0, np.inf, gain / loss)
    df_output['rsi'] = 100 - (100 / (1 + rs))

    # MACD (Moving Average Convergence Divergence)
    exp12 = df_output['c'].ewm(span=12, adjust=False).mean()
    exp26 = df_output['c'].ewm(span=26, adjust=False).mean()
    df_output['macd'] = exp12 - exp26
    df_output['macd_signal'] = df_output['macd'].ewm(span=9, adjust=False).mean()

    return df_output


df = create_features(df)
df = df.replace([np.inf, -np.inf], np.nan)  # Replace inf/-inf with NaN
df = df.dropna()  # Drop rows with NaN values resulting from feature creation

# --- 3. Define Features (X) and Target (y) ---
print("Defining features and target variable...")
# Drop columns not used as features or the target itself
X = df.drop(['next_color', 'current_color', 'ti', 'datetime', 'o', 'h', 'l', 'c', 'v', 'oi'], axis=1, errors='ignore')
y = df['next_color']

# --- 4. Evaluation Metrics Function ---
def evaluate_model(name, y_true, y_pred, y_proba):
    print(f"\n--- {name} Performance ---")
    print(f"Accuracy: {accuracy_score(y_true, y_pred):.4f}")
    print(f"Precision: {precision_score(y_true, y_pred):.4f}")
    print(f"Recall: {recall_score(y_true, y_pred):.4f}")
    print(f"F1 Score: {f1_score(y_true, y_pred):.4f}")
    print(f"ROC AUC: {roc_auc_score(y_true, y_proba):.4f}")
    print("\nClassification Report:")
    print(classification_report(y_true, y_pred))
    print("Confusion Matrix:")
    print(confusion_matrix(y_true, y_pred))


# --- 5. Baseline Accuracy ---
# This serves as a simple comparison point for model performance
print("\n--- Calculating Baseline Accuracy ---")
baseline_acc = max(y.value_counts(normalize=True))
print(f"Baseline Accuracy (most frequent class): {baseline_acc:.4f}")

# --- 6. Time Series Cross-Validation Setup (for Walk-Forward and GridSearchCV) ---
n_splits = 5  # Number of splits for TimeSeriesSplit
tscv = TimeSeriesSplit(n_splits=n_splits)


# --- 7. Hyperparameter Tuning with GridSearchCV (using Multi-processing) ---
# This section performs simulations to find the best parameters

print("\n--- Performing Hyperparameter Tuning for Random Forest (GridSearchCV with Multi-processing) ---")
param_grid_rf = {
    'n_estimators': [100, 200, 300, 400],
    'max_depth': [10, 20, 30, None],
    'min_samples_split': [2, 5, 10],
    'min_samples_leaf': [1, 2, 4]
}
# Total RF combinations: 4 * 4 * 3 * 3 = 144
# Each combination is evaluated 'n_splits' (5) times, so 144 * 5 = 720 simulations

grid_search_rf = GridSearchCV(
    estimator=RandomForestClassifier(random_state=42),
    param_grid=param_grid_rf,
    cv=tscv,  # Use TimeSeriesSplit for proper time-series cross-validation
    n_jobs=-1,  # Use all available CPU cores for parallel processing
    scoring='accuracy',
    verbose=2  # Increased verbosity to see progress
)
grid_search_rf.fit(X, y)

print("\nBest parameters for Random Forest:", grid_search_rf.best_params_)
print("Best cross-validation accuracy for Random Forest:", grid_search_rf.best_score_)

print("\n--- Performing Hyperparameter Tuning for XGBoost (GridSearchCV with Multi-processing) ---")
param_grid_xgb = {
    'n_estimators': [100, 200, 300, 400],
    'learning_rate': [0.01, 0.05, 0.1],
    'max_depth': [3, 5, 7, 9],
    'subsample': [0.7, 0.8, 0.9],
    'colsample_bytree': [0.7, 0.8, 0.9]
}
# Total XGB combinations: 4 * 3 * 4 * 3 * 3 = 432
# Each combination is evaluated 'n_splits' (5) times, so 432 * 5 = 2160 simulations

grid_search_xgb = GridSearchCV(
    estimator=XGBClassifier(random_state=42, eval_metric='logloss'),
    param_grid=param_grid_xgb,
    cv=tscv,  # Use TimeSeriesSplit for proper time-series cross-validation
    n_jobs=-1,  # Use all available CPU cores for parallel processing
    scoring='accuracy',
    verbose=2  # Increased verbosity to see progress
)
grid_search_xgb.fit(X, y)

print("\nBest parameters for XGBoost:", grid_search_xgb.best_params_)
print("Best cross-validation accuracy for XGBoost:", grid_search_xgb.best_score_)


# --- 8. Model-Specific Feature Selection ---
# Different features for different outcomes/models based on their importance

print("\n--- Performing Model-Specific Feature Selection ---")


def get_important_features(model, feature_names, threshold=0.01):
    importances = model.feature_importances_
    important_features = [feature for feature, importance in zip(feature_names, importances) if importance >= threshold]
    return important_features


# Select features for the best Random Forest model
best_rf_model = grid_search_rf.best_estimator_
rf_important_features = get_important_features(best_rf_model, X.columns, threshold=0.01)
print(f"\nRandom Forest Selected Features (importance >= 0.01):")
print(rf_important_features)

# Select features for the best XGBoost model
best_xgb_model = grid_search_xgb.best_estimator_
xgb_important_features = get_important_features(best_xgb_model, X.columns, threshold=0.01)
print(f"\nXGBoost Selected Features (importance >= 0.01):")
print(xgb_important_features)


# --- 9. Walk-Forward Validation with Best Models and Selected Features ---
# This provides a more robust evaluation for time-series data

print("\n--- Performing Walk-Forward Validation with Tuned Models and Selected Features ---")


def walk_forward_validation(X_data, y_data, model_cls, model_kwargs=None, features_to_use=None):
    """
    Performs walk-forward validation and returns out-of-fold predictions and scores.
    Uses TimeSeriesSplit defined globally.
    """
    if model_kwargs is None:
        model_kwargs = {}
    
    # Initialize arrays to store predictions and probabilities across all folds
    oof_preds = np.zeros(len(y_data))
    oof_proba = np.zeros(len(y_data))
    fold_scores = []

    # Use selected features if provided, otherwise use all features in X_data
    X_val = X_data[features_to_use] if features_to_use else X_data

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X_val)):
        X_train, X_test = X_val.iloc[train_idx], X_val.iloc[test_idx]
        y_train, y_test = y_data.iloc[train_idx], y_data.iloc[test_idx]

        model = model_cls(**model_kwargs)
        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        proba = model.predict_proba(X_test)[:, 1]

        # Store predictions and probabilities in their correct out-of-fold positions
        oof_preds[test_idx] = preds
        oof_proba[test_idx] = proba

        acc = accuracy_score(y_test, preds)
        print(f"Fold {fold + 1} Accuracy: {acc:.4f}")
        fold_scores.append(acc)
    return oof_preds, oof_proba, fold_scores


# Evaluate the best Random Forest model with its selected features
rf_best_oof_preds, rf_best_oof_proba, rf_best_scores = walk_forward_validation(
    X, y, RandomForestClassifier, grid_search_rf.best_params_, rf_important_features
)
evaluate_model("Random Forest (Best Params & Selected Features)", y, rf_best_oof_preds, rf_best_oof_proba)

# Evaluate the best XGBoost model with its selected features
# Ensure eval_metric is set for XGBoost if not already in best_params_
xgb_model_kwargs = {**grid_search_xgb.best_params_, 'eval_metric': 'logloss'}
xgb_best_oof_preds, xgb_best_oof_proba, xgb_best_scores = walk_forward_validation(
    X, y, XGBClassifier, xgb_model_kwargs, xgb_important_features
)
evaluate_model("XGBoost (Best Params & Selected Features)", y, xgb_best_oof_preds, xgb_best_oof_proba)

# --- 10. Saving Best Model Details ---
# This part saves the findings so you can easily retrieve them
print("\nSaving best model details to 'best_model_details.txt'...")
with open('best_model_details.txt', 'w') as f:
    f.write("--- Best Random Forest Model Details ---\n")
    f.write(f"Best Parameters: {grid_search_rf.best_params_}\n")
    f.write(f"Best Cross-Validation Accuracy (from GridSearchCV): {grid_search_rf.best_score_:.4f}\n")
    f.write("Selected Features:\n")
    f.write(str(rf_important_features) + "\n\n")
    f.write(f"Walk-Forward Validation Mean Accuracy: {np.mean(rf_best_scores):.4f}\n")
    f.write(f"Walk-Forward Validation Standard Deviation: {np.std(rf_best_scores):.4f}\n")


    f.write("\n--- Best XGBoost Model Details ---\n")
    f.write(f"Best Parameters: {grid_search_xgb.best_params_}\n")
    f.write(f"Best Cross-Validation Accuracy (from GridSearchCV): {grid_search_xgb.best_score_:.4f}\n")
    f.write("Selected Features:\n")
    f.write(str(xgb_important_features) + "\n\n")
    f.write(f"Walk-Forward Validation Mean Accuracy: {np.mean(xgb_best_scores):.4f}\n")
    f.write(f"Walk-Forward Validation Standard Deviation: {np.std(xgb_best_scores):.4f}\n")


print("\nProcess complete. Check 'best_model_details.txt' for a summary of the best models and their features.")