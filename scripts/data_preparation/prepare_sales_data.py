import pathlib
import sys
import pandas as pd
import logging

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# Constants
DATA_DIR: pathlib.Path = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("raw")
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("prepared")

def read_raw_data(input_file: str) -> pd.DataFrame:
    """Read raw sales data from a CSV file."""
    logger.info(f"Reading raw data from {input_file}")
    return pd.read_csv(input_file)

def save_prepared_data(df: pd.DataFrame, output_file: str) -> None:
    """Save the prepared sales data to a CSV file."""
    logger.info(f"Saving prepared data to {output_file}")
    df.to_csv(output_file, index=False)

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows."""
    initial_shape = df.shape
    df = df.drop_duplicates()
    logger.info(f"Removed {initial_shape[0] - df.shape[0]} duplicates")
    return df

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handle missing values in the dataset."""
    df['saleamount'] = df['saleamount'].fillna(df['saleamount'].median())
    df = df.dropna(subset=['customerid'])
    logger.info(f"Handled missing values, new shape: {df.shape}")
    return df

def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize data formats, including date formatting."""
    df['saledate'] = pd.to_datetime(df['saledate'], errors='coerce').dt.strftime('%Y-%m-%d')
    logger.info(f"Standardized date format for 'saledate'")
    return df

def remove_outliers(df: pd.DataFrame, columns: list = ['saleamount']) -> pd.DataFrame:
    """Remove outliers using IQR method."""
    Q1 = df[columns].quantile(0.25)
    Q3 = df[columns].quantile(0.75)
    IQR = Q3 - Q1
    df = df[~((df[columns] < (Q1 - 1.5 * IQR)) | (df[columns] > (Q3 + 1.5 * IQR))).any(axis=1)]
    logger.info(f"Removed outliers from columns: {', '.join(columns)}")
    return df

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate data against business rules."""
    logger.info(f"FUNCTION START: validate_data with dataframe shape={df.shape}")
    
    invalid_saleamount = df[df['saleamount'] < 0]
    logger.info(f"Found {invalid_saleamount.shape[0]} rows with negative sale amounts")
    df = df[df['saleamount'] >= 0]
    
    invalid_discount = df[(df['discountpercent'] < 0) | (df['discountpercent'] > 100)]
    logger.info(f"Found {invalid_discount.shape[0]} rows with invalid discount percentages")
    df = df[(df['discountpercent'] >= 0) & (df['discountpercent'] <= 100)]
    
    logger.info(f"Data validation complete with final shape={df.shape}")
    return df

def main() -> None:
    logger.info("==================================")
    logger.info("STARTING prepare_sales_data.py")
    logger.info("==================================")

    logger.info(f"Root project folder: {PROJECT_ROOT}")
    logger.info(f"data / raw folder: {RAW_DATA_DIR}")
    logger.info(f"data / prepared folder: {PREPARED_DATA_DIR}")

    # Correct the input_file path to use RAW_DATA_DIR
    input_file = RAW_DATA_DIR.joinpath("sales_data.csv")
    output_file = PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv")

    df = read_raw_data(input_file)

    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")

    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    changed_columns = [f"{old} -> {new}" for old, new in zip(original_columns, df.columns) if old != new]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = standardize_formats(df)
    df = remove_outliers(df)
    df = validate_data(df)

    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info("FINISHED prepare_sales_data.py")
    logger.info("==================================")

if __name__ == "__main__":
    main()
