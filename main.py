from Cleaning.cleaning import *
from Validations.validations import *
from tkinter.filedialog import askopenfile 

file_path = askopenfile()
DataPross = DataProcessor(file_path)

df = DataPross.process_file()
DataProcessor.get_file_type(df=df)


if DataPross.file_type == "API":
    validations = Validations_API(df,file_path)
    validations.export_to_excel()
elif DataPross.file_type == "TAG":
    validations = Validations_TAG(df,file_path)
    validations.export_to_excel()
else:
    print("Couldnt assign Technology (API, TAG or Amazon) to sample ran")

print("Report Ready!")
