import pandas as pd
import xml.etree.ElementTree as ET
import json
import os
import numpy as np
import re
import string

class DataProcessor:
    """
    Processes data files to determine their type based on column names.

    Attributes:
        file_path (str): The path to the data file.
        file_type (str or None): The type of the file, either 'API', 'TAG', or None.
    """

    def __init__(self, file_path) -> None:
        """
        Initializes the DataProcessor with a file path.

        Args:
            file_path (str): The path to the data file.
        """
        self.file_path = file_path
        self.file_type = None

    def get_file_type(self, df) -> str:
        """
        Determines the type of the file based on its columns.

        Args:
            df (pandas.DataFrame): The DataFrame to analyze.

        Returns:
            str: The determined file type ('API', 'TAG', 'AMAZON', or None).
        """
        columns = set(col.lower() for col in df.columns)

        match columns:
            case cols if 'id_api_hit' in cols or 'dt_transaction' in cols:
                self.file_type = 'API'
            case cols if 'id_log' in cols or 'datacomp' in cols:
                self.file_type = 'TAG'
            case cols if 'asin' in cols or 'postal_code' in cols:
                self.file_type = 'AMAZON'
            case _:
                self.file_type = None

        return self.file_type

    def process_file(self) -> pd.DataFrame:
        """
        Processes the file based on its extension and returns a cleaned DataFrame.

        Supported formats:
            - CSV
            - XML
            - JSON
            - Excel (.xls, .xlsx)

        The method identifies the file type by checking specific column names and applies appropriate converters if needed.

        Returns:
            pandas.DataFrame: A DataFrame containing the processed data with empty strings replaced by `pd.NA`.

        Raises:
            ValueError: If the file format is not supported.
        """
        file_extension = os.path.splitext(self.file_path)[1].lower()
        
        if file_extension == '.csv':            
            headers = pd.read_csv(self.file_path, nrows=0)            
            if 'id_api_hit'.lower() in headers.columns or 'dt_transaction'.lower() in headers.columns:
                converters = self.get_converters('API')
            elif 'id_log'.lower() in headers.columns or 'datacomp'.lower() in headers.columns: 
                converters = self.get_converters('TAG') 

            df = pd.read_csv(self.file_path, converters=converters)            
        
        elif file_extension == '.xml':
            tree = ET.parse(self.file_path)
            root = tree.getroot()
            data = []
            for item in root:
                row = {}
                for child in item:
                    row[child.tag] = child.text
                data.append(row)
            df = pd.DataFrame(data)            
        
        elif file_extension == '.json':
            with open(self.file_path, 'r') as file:
                data = json.load(file)
            df = pd.DataFrame(data)            
        
        elif file_extension in ['.xls', '.xlsx']:
            headers = pd.read_excel(self.file_path, nrows=0)            
            if 'id_api_hit'.lower() in headers.columns or 'dt_transaction'.lower() in headers.columns:
                converters = self.get_converters('API')
            elif 'id_log'.lower() in headers.columns or 'datacomp'.lower() in headers.columns: 
                converters = self.get_converters('TAG') 

            df = pd.read_excel(self.file_path, converters=converters)

        elif file_extension == 'txt000':
            headers = pd.read_csv(self.file_path, sep= "|",nrows = 0)
            converters = self.get_converters("AMAZON")
            
        else:
            raise ValueError("Unsupported file format")
        
        df = df.replace('', pd.NA)
        return df

    def get_converters(self, type: str) -> dict:
        """
        Returns a dictionary of column converters based on the file type.

        The converters ensure that specific columns are read with the correct data types,
        such as strings, numerics, or datetimes. This helps standardize data ingestion
        across different file formats and schemas.

        Args:
            type (str): The type of the file. Must be either 'TAG' or 'API'.

        Returns:
            dict: A dictionary mapping column names to conversion functions or types.

        Raises:
            ValueError: If the provided file type is not recognized.
        """

        if type == 'TAG':
            return {
                'id_log': str,
                'carrinho': str,
                'transactionid': str,
                'plataform': str,
                'storeid': str,
                'nm_brand': str,
                'nm_category_l5': str,
                'ean': str,
                'nm_manufacturer': str,
                'mktsaleid': str,
                'productname': str,
                'sku': str,
                'nm_subbrand': str,
                'value': str,
                'zipcode': str,
                'gender': str,
                'productcondition': str,
                'quantity': str,
                #numeric columns
                'deliverytax': lambda x : pd.to_numeric(x, errors='coerce'),
                'deliverytime': lambda x : pd.to_numeric(x, errors='coerce'),
                'deliverytype': lambda x : pd.to_numeric(x, errors='coerce'),
                'parcels': lambda x : pd.to_numeric(x, errors='coerce'),
                'paymenttype': lambda x : pd.to_numeric(x, errors='coerce'),                
                'cardflag': lambda x : pd.to_numeric(x, errors='coerce'),
                'invoiceemissor': lambda x : pd.to_numeric(x, errors='coerce'),
                'totalspent': lambda x : pd.to_numeric(x, errors='coerce'),
                #datetime columns
                'datacomp': lambda x: pd.to_datetime(x, errors='coerce'),
                'birthday': lambda x: pd.to_datetime(x, errors='coerce')                   
            }
        elif type == 'API':
            return {
                'id_api_hit': str,
                'id_store': str,
                'id_transaction': str,
                'dt_transaction': lambda x: pd.to_datetime(x, errors='coerce'),
                'nm_platform': str,
                'nm_gender': str,
                'cd_zipcode': str,
                'qt_parcel': lambda x: pd.to_numeric(x, errors='coerce'),                                
                'vl_totalspent': lambda x: pd.to_numeric(x, errors='coerce'),
                'cd_paymenttype': lambda x: pd.to_numeric(x, errors='coerce'),
                'cd_cardflag': lambda x: pd.to_numeric(x, errors='coerce'),
                'cd_invoiceemissor': lambda x: pd.to_numeric(x, errors='coerce'),
                'nm_age': lambda x: pd.to_numeric(x, errors='coerce'), 
                'nm_birthday': lambda x: pd.to_datetime(x, errors='coerce'),
                'nm_lastmile': lambda x: pd.to_numeric(x, errors='coerce'),
                'id_log': str,
                'dt_process_header': lambda x: pd.to_datetime(x, errors='coerce'),
                'dt_process_detail': lambda x: pd.to_datetime(x, errors='coerce'),
                'dt_import':  lambda x: pd.to_datetime(x, errors='coerce'),
                'cd_sku': str,
                'cd_ean': str,
                'nm_product': str,
                'vl_product': lambda x : pd.to_numeric(x, errors='coerce'),
                'qt_product': lambda x: pd.to_numeric(x, errors='coerce'),
                'cd_productcondition': lambda x: pd.to_numeric(x, errors='coerce'),
                'nm_deliverytype': lambda x: pd.to_numeric(x, errors='coerce'),
                'vl_deliverytax': lambda x: pd.to_numeric(x, errors='coerce'),
                'qt_deliverytime': lambda x: pd.to_numeric(x, errors='coerce'),
                'nm_mktsaleid': str,
                'nm_model': str,
                'nm_manufacturer': str,
                'nm_brand': str,
                'nm_subbrand': str,
                'nm_catl1': str,
                'nm_catl2': str,
                'nm_catl3': str,
                'nm_catl4': str,
                'nm_catl5': str,
                'tx_fulldescription': str,
                'tx_fullpath': str,
                'cd_pack': lambda x: pd.to_numeric(x, errors='coerce'),
                'cd_promo': lambda x: pd.to_numeric(x, errors='coerce'),
                'cd_ownbrand': lambda x: pd.to_numeric(x, errors='coerce'),
                'cd_intll': lambda x: pd.to_numeric(x, errors='coerce'),
                'nm_origin': str 
            }
        elif type == 'AMAZON':
            return {
                'asin' : str,
                'ean1' : str,
                'dest_country' : str,
                'item_name': str,
                'source_country':str,
                'date': lambda x: pd.to_datetime(x,errors='coerce'),
                'date_granularity': str,
                'business_group': str,
                'postal_code': str,
                'base_currency_code': str,
                'our_price': float,
                'distinct_order_count': float,
                'shipped_units' : float,
                'shipped_sales': float,
                'shipped_sales_w_tax': float,
                'shipped_sales_after_discount': float,
                'shipped_sales_w_tax_after_discount': float,
                'promotion': str

            }
        else:
            raise ValueError(f"File type unknown: {type}")