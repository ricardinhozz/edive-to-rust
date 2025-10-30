import pandas as pd
from Cleaning.cleaning import DataProcessor
from Configs import Configs
import datetime
import os
import numpy as pd  

output_folder = Configs.output_folder
if not os.path.exists(output_folder):
    os.mkdir(f'({output_folder})')

timestamp = datetime.datetime.today().strftime('%Y-%m-%d_%H%M%S')

class Validations_TAG:
    """
    Handles validation logic for TAG-type data files.

    This class initializes with a DataFrame and its file path, extracts metadata such as the eTailer name,
    and prepares structures for storing validation results. Upon instantiation, it automatically triggers
    the validation workflow via the `run_methods()` function.

    Attributes:
        df (pandas.DataFrame): The input data to be validated.
        file_path (str): The path to the input file.
        etailer_name (str): The name of the eTailer, extracted from the file name.
        output_dict (dict): A dictionary to store intermediate or final validation outputs.
        raw_data (pandas.DataFrame): A copy of the original input data.
    """

    def __init__(self,df,file_path:str):
        self.df = df
        self.file_path = file_path
        self.etailer_name = self.file_path.split('/')[-1].split('.')[0]
        self.output_dict = {}
        self.raw_data = df
        self.run_methods()

    def run_methods(self) -> None:
        """
        Executes a predefined sequence of validation and analysis methods on the raw dataset.

        This method:
        - Iterates through a list of validation and analysis methods.
        - Applies each method to the raw dataset (`self.raw_data`).
        - Catches and logs any exceptions that occur during method execution.
        - Stores the results or error messages in the output dictionary.
        - Sorts the output dictionary by index for organized access.

        Args:
            None: The method uses `self.raw_data` internally.

        Returns:
            None: All results are stored in `self.output_dict`.
        """

        df_raw = self.raw_data
        methods = [
            """
            self.columns_completness,
            self.sales_validation,
            self.paymenttype_validation,
            self.size_comparison,
            self.totalspent_match_total,
            self.zipcode_length,
            self.duplicated_ids,
            self.wrong_columns_with_pipe,
            self.sales_validation_split,
            self.repeated_productname,
            #self.ean_productname_validation,
            #self.sku_productname_validation,
            #self.test_validation,
            self.platform_conformity,
            self.gender_conformity,
            self.parcels_conformity,
            self.value_conformity,
            self.quantity_conformity,
            self.totalspent_conformity,
            self.deliverytax_conformity,
            self.deliverytime_conformity,
            self.deliverytype_conformity,
            self.paymenttype_conformity,
            self.total_rows,
            self.total_columns,
            self.first_day,
            self.last_day,
            self.missing_days,
            self.prodcondition_conformity,
            self.invoiceemissor_conformity,
            self.cardflag_conformity,
            self.duplicated_all,
            self.totalspent_threshold,
            self.totalspent_outlier,
            self.undefined_count,
            self.marketplace_analysis,
            self.storeid_null
"""
        ]

        for index, method in enumerate(methods, start=0):
            try:
                method(df_raw)
            except Exception as e:
                self.output_dict[index] = [method.__name__, df_raw.shape[0], str(e), 'Error']
        self.output_dict = dict(sorted(self.output_dict.items())) 

    def export_to_excel(self) -> None:
        """
        Exports the validation results stored in `self.output_dict` to a formatted Excel file.

        Export logic:
        - Creates an Excel file named using the etailer name and timestamp.
        - Builds a summary sheet containing:
        - Index of validations
        - Validation labels
        - Number of occurrences
        - Validation types
        - Hyperlinks to individual validation sheets
        - Applies custom formatting to headers, cells, and conditional formatting based on validation type.
        - Writes individual sheets for each validation, including either the DataFrame output or error message.
        - Adds a "Back to Summary" link on each sheet for easy navigation.

        This method provides a structured and visually enhanced Excel report for reviewing validation results.

        Returns:
            None: The Excel file is saved to the specified output folder.
        """

        with pd.ExcelWriter(output_folder + self.etailer_name + f'{timestamp}' + '.xlsx', engine='xlsxwriter') as writer:
            summary_data = {
                'INDEX': list(self.output_dict.keys()),
                'Validation': [value[0] for value in self.output_dict.values()],
                'Ocurrences': [value[1] for value in self.output_dict.values()],
                'Validation Type': [value[3] for value in self.output_dict.values()],
                'Go To': [f'=HYPERLINK("#\'{key} - {value[0]}\'!A1", "Sample")' for key, value in self.output_dict.items()]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False, startrow=7, startcol=0)

            wb = writer.book
            wsheet = writer.sheets['Summary']

            #formatting cells
            cell_format_title = wb.add_format({'bold': True, 'font_color': 'white', 'bg_color': '#2C6DF6', 'border': 1, 'border_color': '#B7DEE8', 'font_name': 'Segoe UI'})
            cell_format_title.set_align('center')
            cell_format = wb.add_format({'border': 1, 'border_color': '#B7DEE8', 'font_size': 12, 'font_name': 'Segoe UI Semibold', 'font_color': '#2C6DF6'})

            text_format3 = wb.add_format({'font_size': 12, 'font_name': 'Segoe UI'})
            text_format1 = wb.add_format({'bold': True, 'font_size': 14, 'font_name': 'Segoe UI'})
            
            green_format = wb.add_format({'align': 'center', 'italic': True, 'bold': True, 'border': 1, 'font_color': 'white', 'border_color': '#B7DEE8', 'font_name': 'Segoe UI', 'font_size': 10, 'bg_color': '#00B050'})
            red_format = wb.add_format({'align': 'center', 'italic': True, 'bold': True, 'border': 1, 'font_color': 'white', 'border_color': '#B7DEE8', 'font_name': 'Segoe UI', 'font_size': 10, 'bg_color': '#FF0000'})
            yellow_format = wb.add_format({'align': 'center', 'italic': True, 'bold': True, 'border': 1, 'font_color': 'white', 'border_color': '#B7DEE8', 'font_name': 'Segoe UI', 'font_size': 10, 'bg_color': '#7030A0'})

            sample_format = wb.add_format({'font_color': '#538DD5','underline':  1,'font_size':  10,'align':'center','italic':True,'border':1,'border_color':'#B7DEE8','font_name':'Segoe UI'})

            # add information
            wsheet.hide_gridlines(2)
            wsheet.write(1, 0, 'eDive Report', text_format1) 
            wsheet.write(2, 0, f'name:  {self.etailer_name.lower()}', text_format3) 
            wsheet.write(3, 0, f'User: {Configs.user}')  

            #conditional formatting
            wsheet.conditional_format('D1:D1048576', {'type': 'cell', 'criteria': '==', 'value': '"Error"', 'format': red_format})
            wsheet.conditional_format('D1:D1048576', {'type': 'cell', 'criteria': '==', 'value': '"Consistency"', 'format': green_format})
            wsheet.conditional_format('D1:D1048576', {'type': 'cell', 'criteria': '==', 'value': '"Conformity"', 'format': green_format})
            wsheet.conditional_format('D1:D1048576', {'type': 'cell', 'criteria': '==', 'value': '"Completeness"', 'format': green_format})
            wsheet.conditional_format('D1:D1048576', {'type': 'cell', 'criteria': '==', 'value': '"Compliance"', 'format': green_format})
            wsheet.conditional_format('D1:D1048576', {'type': 'cell', 'criteria': '==', 'value': '"Info"', 'format': yellow_format})

            for col_num, col in enumerate(summary_df.columns):
                max_len = max(summary_df[col].astype(str).map(len).max(), len(col)) + 2  # extra space
                wsheet.set_column(col_num, col_num, max_len)  # startcol=0 adjust

            # write headers with formatting
            for col_num, value in enumerate(summary_df.columns.values):
                wsheet.write(7, col_num, value, cell_format_title)

            # write data with formatting
            for row_num in range(8, 8 + len(summary_df)):
                for col_num in range(len(summary_df.columns)):
                    cell_value = summary_df.iloc[row_num - 8, col_num]
                    if isinstance(cell_value, (int, float, str)):
                        wsheet.write(row_num, col_num, cell_value, cell_format)
                    else:
                        wsheet.write(row_num, col_num, str(cell_value), cell_format)

            for key, value in self.output_dict.items():
                sheet_name = f'{key} - {value[0]}'
                if isinstance(value[2], pd.DataFrame):
                    value[2].to_excel(writer, sheet_name=sheet_name)
                else:
                    error_df = pd.DataFrame({'Error': [value[2]]})
                    error_df.to_excel(writer, sheet_name=sheet_name)

            for sheet_name in writer.sheets:
                if sheet_name != 'Summary':
                    worksheet = writer.sheets[sheet_name]
                    worksheet.write_url('A1', "internal:'Summary'!A1", string='Back to Summary', cell_format=cell_format_title)


class Validations_API:

    def __init__(self,df,file_path):
        """
        Initializes the Validations_API class with the provided DataFrame and file path.

        This constructor sets up the initial state of the class, including:
        - Storing the raw DataFrame and file path.
        - Extracting the e-tailer name from the file path.
        - Initializing an empty dictionary to store validation outputs.
        - Keeping a copy of the raw data for reference.
        - Automatically triggering the execution of validation methods.

        Args:
            df (pandas.DataFrame): The DataFrame containing transaction data.
            file_path (str): The path to the file from which the data was loaded.

        Returns:
            None
        """

        self.df = df
        self.file_path = file_path
        self.etailer_name = self.file_path.split('/')[-1].split('.')[0]
        self.output_dict = {}
        self.raw_data = df
        self.run_methods()


    def run_methods(self) -> None:
            """
            Executes a predefined sequence of validation methods on the raw transaction data.

            Execution logic:
            - Retrieves the raw transaction data stored in `self.raw_data`.
            - Defines a list of validation methods to be executed in order.
            - Iterates through each method, applying it to the raw data.
            - If a method raises an exception during execution, the error is captured and stored in `self.output_dict` with relevant metadata.
            - After all methods are executed, the output dictionary is sorted by index for organized reporting.

            This method serves as the central runner for all data validation checks, ensuring consistency and completeness across the dataset.

            Returns:
                None: All validation results and errors are stored in `self.output_dict`.
            """


            df_raw = self.raw_data

            methods = [
                self.columns_completness,
                self.sales_validation,
                self.paymenttype_validation,
                self.totalspent_match_total,
                self.zipcode_length,
                self.duplicated_ids,
                self.sales_validation_split,
                self.platform_conformity,
                self.value_conformity,
                self.quantity_conformity,
                self.totalspent_conformity,
                self.deliverytax_conformity,
                self.deliverytime_conformity,
                self.deliverytype_conformity,
                self.paymenttype_conformity,
                self.total_rows,
                self.total_columns,
                self.first_day,
                self.last_day,
                self.missing_days,
                self.cardflag_conformity,
                self.invoiceemissor_conformity,
                self.productcondition_conformity,
                self.duplicated_all,
                self.totalspent_threshold,
                self.totalspent_outlier,
                self.undefined_count,
                self.marketplace_analysis
            ]

            for index, method in enumerate(methods, start=0):
                try:
                    method(df_raw)
                except Exception as e:
                    self.output_dict[index] = [method.__name__, df_raw.shape[0], str(e), 'Error']
            self.output_dict = dict(sorted(self.output_dict.items())) 

    def export_to_excel(self) -> None:

        """
        Exports the validation results stored in `self.output_dict` to a formatted Excel file.

        Export logic:
        - Creates an Excel file named using the etailer name and timestamp.
        - Builds a summary sheet containing:
            1. Index of validations
            2. Validation labels
            3. Number of occurrences
            4. Validation types
            5. Hyperlinks to individual validation sheets
        - Applies custom formatting to headers, cells, and conditional formatting based on validation type.
        - Writes individual sheets for each validation, including either the DataFrame output or error message.
        - Adds a "Back to Summary" link on each sheet for easy navigation.

        This method provides a structured and visually enhanced Excel report for reviewing validation results.

        Returns:
            None: The Excel file is saved to the specified output folder.
        """

        with pd.ExcelWriter(output_folder + self.etailer_name + f'{timestamp}' + '.xlsx', engine='xlsxwriter') as writer:
            summary_data = {
                'INDEX': list(self.output_dict.keys()),
                'Validation': [value[0] for value in self.output_dict.values()],
                'Ocurrences': [value[1] for value in self.output_dict.values()],
                'Validation Type': [value[3] for value in self.output_dict.values()],
                'Go To': [f'=HYPERLINK("#\'{key} - {value[0]}\'!A1", "Sample")' for key, value in self.output_dict.items()]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False, startrow=7, startcol=0)

            wb = writer.book
            wsheet = writer.sheets['Summary']

            #formatting cells
            cell_format_title = wb.add_format({'bold': True, 'font_color': 'white', 'bg_color': '#2C6DF6', 'border': 1, 'border_color': '#B7DEE8', 'font_name': 'Segoe UI'})
            cell_format_title.set_align('center')
            cell_format = wb.add_format({'border': 1, 'border_color': '#B7DEE8', 'font_size': 12, 'font_name': 'Segoe UI Semibold', 'font_color': '#2C6DF6'})

            text_format3 = wb.add_format({'font_size': 12, 'font_name': 'Segoe UI'})
            text_format1 = wb.add_format({'bold': True, 'font_size': 14, 'font_name': 'Segoe UI'})
            
            green_format = wb.add_format({'align': 'center', 'italic': True, 'bold': True, 'border': 1, 'font_color': 'white', 'border_color': '#B7DEE8', 'font_name': 'Segoe UI', 'font_size': 10, 'bg_color': '#00B050'})
            red_format = wb.add_format({'align': 'center', 'italic': True, 'bold': True, 'border': 1, 'font_color': 'white', 'border_color': '#B7DEE8', 'font_name': 'Segoe UI', 'font_size': 10, 'bg_color': '#FF0000'})
            yellow_format = wb.add_format({'align': 'center', 'italic': True, 'bold': True, 'border': 1, 'font_color': 'white', 'border_color': '#B7DEE8', 'font_name': 'Segoe UI', 'font_size': 10, 'bg_color': '#7030A0'})

            sample_format = wb.add_format({'font_color': '#538DD5','underline':  1,'font_size':  10,'align':'center','italic':True,'border':1,'border_color':'#B7DEE8','font_name':'Segoe UI'})

            # add information
            wsheet.hide_gridlines(2)
            wsheet.write(1, 0, 'eDive Report', text_format1) 
            wsheet.write(2, 0, f'name:  {self.etailer_name.lower()}', text_format3) 
            wsheet.write(3, 0, f'User: {Configs.user}')  

            #conditional formatting
            wsheet.conditional_format('D1:D1048576', {'type': 'cell', 'criteria': '==', 'value': '"Error"', 'format': red_format})
            wsheet.conditional_format('D1:D1048576', {'type': 'cell', 'criteria': '==', 'value': '"Consistency"', 'format': green_format})
            wsheet.conditional_format('D1:D1048576', {'type': 'cell', 'criteria': '==', 'value': '"Conformity"', 'format': green_format})
            wsheet.conditional_format('D1:D1048576', {'type': 'cell', 'criteria': '==', 'value': '"Completeness"', 'format': green_format})
            wsheet.conditional_format('D1:D1048576', {'type': 'cell', 'criteria': '==', 'value': '"Compliance"', 'format': green_format})
            wsheet.conditional_format('D1:D1048576', {'type': 'cell', 'criteria': '==', 'value': '"Info"', 'format': yellow_format})

            for col_num, col in enumerate(summary_df.columns):
                max_len = max(summary_df[col].astype(str).map(len).max(), len(col)) + 2  # extra space
                wsheet.set_column(col_num, col_num, max_len)  # startcol=0 adjust

            # write headers with formatting
            for col_num, value in enumerate(summary_df.columns.values):
                wsheet.write(7, col_num, value, cell_format_title)

            # write data with formatting
            for row_num in range(8, 8 + len(summary_df)):
                for col_num in range(len(summary_df.columns)):
                    cell_value = summary_df.iloc[row_num - 8, col_num]
                    if isinstance(cell_value, (int, float, str)):
                        wsheet.write(row_num, col_num, cell_value, cell_format)
                    else:
                        wsheet.write(row_num, col_num, str(cell_value), cell_format)

            for key, value in self.output_dict.items():
                sheet_name = f'{key} - {value[0]}'
                if isinstance(value[2], pd.DataFrame):
                    value[2].to_excel(writer, sheet_name=sheet_name)
                else:
                    error_df = pd.DataFrame({'Error': [value[2]]})
                    error_df.to_excel(writer, sheet_name=sheet_name)

            for sheet_name in writer.sheets:
                if sheet_name != 'Summary':
                    worksheet = writer.sheets[sheet_name]
                    worksheet.write_url('A1', "internal:'Summary'!A1", string='Back to Summary', cell_format=cell_format_title)