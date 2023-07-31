import pandas as pd
import glob
import os
import extract


class DateTransformer(extract.FileExtractor):
    """
    A class for transforming data by adding date-related columns. 
    This is a subclass of the FileExtractor class.

    Attributes:
        path_output (str): The path to the input directory containing the source files.

    Methods:
        add_columns: Adds date-related columns to the source files.

    Note:
        Before using the DateTransformer class, make sure the input directory exists and contains valid files.
    """

    def __init__(self, path_input, path_input_clean, extension, batch, path_output):
        """
        Initialization of the DateTransformer instance.

        Args:
            path_input (str): source directory 
            path_input_clean (str): destination directory
            extension (str): define the file type
            batch (str): define de lot
            path_output (str): final file directory
        """
        super().__init__(path_input, path_input_clean, extension, batch)
        self.path_output = path_output

    def add_columns(self, an, luna):
        """
        Adds date-related columns to the source files.

        This method reads each source file, adds date-related columns such as 'AN' and 'LUNA',
        and saves the transformed file with the added columns.

        Args:
            an_rap (int): The year value to be added to the 'AN' column.
            luna_rap (int): The month value to be added to the 'LUNA' column.

        Note:
            Before using this method, ensure that the input directory contains valid source files.

        Raises:
            Exception: If there is any error in files, directory, ...
        """
        try:
            os.chdir(self.path_input_clean)
            all_filenames = [i for i in glob.glob(f"*.{self.extension}")]

            for f in all_filenames:
                print(f"File transformed: {f}")
                df = pd.read_csv(
                    f,
                    delimiter="|",
                    engine="python",
                    decimal=",",
                    encoding="unicode_escape",
                )

                df.columns = df.columns.str.upper()
                df["AN"] = an
                df["LUNA"] = luna

                df.update(
                    df[
                        [
                            'UNICOM_PARAM',
                            'DENUMERE_CLIENT',
                            'TIP_CLIENT',
                            'SEGMENT_EE',
                            'SEGMENT_GN',
                            'ETICHETA_REGIUNE',
                            'MACRO_ZONA_CLIENT',
                            'MACRO_DEPARTAMENT_GESTIONARE',
                            'TRANSA_EE',
                            'TRANSA_GN',
                            'ETICHETA_PORTOFOLIU',
                            'COD_UNICOM',
                            'NOM_UNICOM',
                            'COD_TAR',
                            'TARIF',
                            'DATA_INTRARE_VIGOARE',
                            'DATA_REZILIERE',
                            'DATA_ESTIMARE_REZILIERE',
                            'DATA_MODIFICARE',
                            'CML_CALCULAT',
                            'CML_CALCULAT_SECV_PREC',
                            'CLI_TRANSF_DIN_REGLEMENTAT',
                            'PERIOADA_VALABILTIATE_CONV',
                            'CONVENTIE_CONSUM_LUNA_1',
                            'CONVENTIE_CONSUM_LUNA_2',
                            'CONVENTIE_CONSUM_LUNA_3',
                            'CONVENTIE_CONSUM_LUNA_4',
                            'CONVENTIE_CONSUM_LUNA_5',
                            'CONVENTIE_CONSUM_LUNA_6',
                            'CONVENTIE_CONSUM_LUNA_7',
                            'CONVENTIE_CONSUM_LUNA_8',
                            'CONVENTIE_CONSUM_LUNA_9',
                            'CONVENTIE_CONSUM_LUNA_10',
                            'CONVENTIE_CONSUM_LUNA_11',
                            'CONVENTIE_CONSUM_LUNA_12',
                            'COD_CLI',
                            'NIS_RAD',
                            'SEC_NIS',
                            'EST_SUM',
                            'STARE_CONTRACT',
                            'PRET',
                            'PRET2',
                            'PRET_REZERVARE',
                            'CUI',
                            'PRET_TARIF_DISTRIBUTIE',
                            'VALOARE_DIAGARA_CONSUM_ZONA1',
                            'VALOARE_DIAGARA_CONSUM_ZONA2',
                            'PRET_INTRODUCERE_RETEA',
                            'PRET_EXTRAJERE_RETEA',
                            'PRET_TARIF_SISTEM',
                            'PRET_TRANSPORT_GAZ',
                            'DATA_ACTIVARE_FACT_ELECTR',
                            'POD',
                            'COD_DZ',
                            'ZONA_DISTRIBUTIE',
                            'CATEG',
                        ]
                    ].fillna(0)
                )

                df.to_csv(
                    os.path.join(self.path_output, f"{f}.csv"),
                    sep="|",
                    header=True,
                    mode="w",
                    index=False,
                    decimal=".",
                )
        except Exception as msg:
            print(f"Error occurred during new column definition => {str(msg)}")
