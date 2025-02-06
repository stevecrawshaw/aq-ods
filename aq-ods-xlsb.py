# %%
from pathlib import Path

from aspose.cells import Workbook

# %%
data_dir = Path("data")
xlsb_files = list(data_dir.glob("*.xlsb"))


# %%
def convert_xlsb_to_xlsx(file_path):
    """Converts an xlsb file to xlsx format."""
    posix_fp = file_path.as_posix()
    workbook = Workbook(posix_fp)
    workbook.save(str(file_path.with_suffix(".xlsx")))


# workbook = Workbook("data/Bristol DTTool_Entriesv4.0.xlsb")
# workbook.save("data/Bristol DTTool_Entriesv4.0.xlsx")

# %%

for xlsb_file in xlsb_files:
    convert_xlsb_to_xlsx(xlsb_file)
