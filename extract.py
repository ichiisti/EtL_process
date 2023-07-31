import pandas as pd
import glob
import os
import gen_trasform as tf


class FileExtractor:
    """
    FileExtractor class is extracting files from a source directory and moving them to a destination directory.

    Attributes: 
        path_input => source directory 
        path_input_clean => destination directory
        extension => define the file type
        batch => define de lot

    Methods:
        extract: Extracts files from the source directory and moves them to the destination director.

    Note:
        Before using the FileExtractor class, make sure the input directory exists and contains valid files.
    """

    def __init__(self, path_input, path_input_clean, extension, batch):
        """
        Initialization of the FileExtractor instance.

        Args:
            path_input (str): source directory 
            path_input_clean (str): destination directory
            extension (str): define the file type
            batch (str): define de lot
        """
        self.path_input = path_input
        self.path_input_clean = path_input_clean
        self.extension = extension
        self.batch = batch
        self.__check_param()

    def __check_param(self):

        if not os.path.exists(self.path_input):
            raise FileNotFoundError(
                f"The specific input for {self.path_input} does not exist !")

        if not os.path.exists(self.path_input_clean):
            raise FileNotFoundError(
                f"The specific input for {self.path_input_clean} does not exist !")

    def __file_rename(self):

        try:
            for file in os.listdir(self.path_input):
                os.rename(os.path.join(self.path_input, file), os.path.join(
                    self.path_input, file.upper())),
                print(f"File renamed : {file}")

        except Exception as msg:
            print(f"Error occurred during file renaming => {str(msg)})")

    def __transform_txt_to_csv(self):
        """
        Transform all text files in the input directory to CSV format with delimiter "|".
        """
        try:
            os.chdir(self.path_input)

            all_files = [i for i in glob.glob(f"*.txt")]

            for f in all_files:
                print(f)

                df = pd.read_csv(f,
                                 delimiter="\t",
                                 engine="python",
                                 decimal=",",
                                 encoding="unicode_escape")

                df.to_csv(
                    os.path.join(self.path_input, f"{f[:-4]}.csv"),
                    sep=",",
                    header=True,
                    mode="w",
                    index=False,
                    decimal=".",
                )
        except Exception as msg:
            print(
                f"Error occurred during transforming txt to csv => {str(msg)}")

    def __delete_txt_files(self):
        """
        Delete all files in the specified directory that end with the extension ".txt".
        """
        try:
            for filename in os.listdir(self.path_input):
                if filename.endswith(".txt"):
                    file_path = os.path.join(self.path_input, filename)
                    os.remove(file_path)
        except Exception as msg:
            print(f"Error occurred during deleting txt files => {str(msg)}")

    def __file_split(self, source_path, file_name_out):

        try:
            for index, chunk in enumerate(
                pd.read_csv(
                    source_path,
                    usecols=tf.sel_col,
                    chunksize=self.batch,
                    engine="python",
                    encoding="unicode_escape",
                    delimiter=",",
                    decimal=".",
                )
            ):
                chunk.columns = chunk.columns.str.lower()
                chunk["SURSA"] = file_name_out
                chunk.to_csv(
                    os.path.join(
                        self.path_input_clean, f"{file_name_out}_chunk{index}.csv"),
                    index=False,
                    sep="|",
                    header=True,
                    mode="w",
                    decimal=".",
                )
        except Exception as msg:
            print(f"Error occurred during file split => {str(msg)}")

    def __extract_data(self):
        try:
            os.chdir(self.path_input)
            all_filenames = [i for i in glob.glob(f"*.{self.extension}")]

            for f in all_filenames:
                print(f"Data extracted from file : {f} ")
                self.__file_split(f, f)
        except Exception as msg:
            print(f"Error occurred during data extraction => {str(msg)}")

    def extract(self):
        """
        Extracts files from the source directory and moves them to the destination directory.

        This method loops through all files in the source directory and moves each file
        to the destination directory.

        Note:
            Before using this method, make sure the source and destination directories exist.

        Raises:
            FileNotFoundError: If the source directory does not exist.
            PermissionError: If there is an issue accessing or moving the files.
        """
        try:
            print("01 - CONVERT FILES AND CLEAN DIRECTPORY !")
            self.__transform_txt_to_csv()
            self.__delete_txt_files()
            print("02 - RENAME FILES !")
            self.__file_rename()
            print("03 - EXTRACT DATA !")
            self.__extract_data()
        except Exception as msg:
            print(f"Error occurred during extraction => {str(msg)}")
