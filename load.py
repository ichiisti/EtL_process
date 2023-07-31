import pandas as pd
import glob
import os
import db_conn as db
import gen_trasform as tf


class DateLoader:
    """
    A class for loading data into database. 

    Attributes:
        path_output (str): The path to the input directory containing the source files.
        extension => define the file type.
        an (int): The year value for the reporting period.
        luna (int): The month value for the reporting period.

    Methods:
        delete_from_tbl: delete data from database.
        load: insert records into the database.

    Note:
        Before using the DateTransformer class, make sure the input directory exists and contains valid files.
    """

    __tbl_name = "contr_activ_int_nlc"

    def __init__(self, path_output, extension, an, luna):
        """
        Initialization of the DateLoader instance.

        Args:
            path_output (str): The path to the input directory containing the source files.
            extension => define the file type.
            an (int): The year value for the reporting period.
            luna (int): The month value for the reporting period.
        """
        self.path_output = path_output
        self.extension = extension
        self.an = an
        self.luna = luna

    def __get_column_names(self):
        try:
            sql_stm = ("SELECT column_name FROM user_tab_columns "
                       "WHERE table_name = upper(" +
                       tf.single_quote(DateLoader.__tbl_name) + ") "
                       "AND column_name not in ('ID','MOD_DE','MOD_TIMP' "
                       ") ORDER BY column_id asc"
                       )

            with db.oracle_sql() as conn:
                col_name = pd.read_sql(sql_stm, conn.connector)

            col_name_list = col_name["COLUMN_NAME"].tolist()

            return col_name_list

        except Exception as msg:
            print(
                f"Error occurred while getting columns from db => {str(msg)}")

    def delete_from_tbl(self):
        """
        Delete data from the database, based on the parameters an, luna and var_no.

        Raises:
            Exception: if the connection to db failed.

        """
        try:
            sql_cmd = (
                "DELETE FROM "
                + DateLoader.__tbl_name
                + " WHERE an = "
                + tf.single_quote(str(self.an))
                + " and luna = "
                + tf.single_quote(str(self.luna))
            )

            with db.oracle_conn() as conn:
                conn.execute(sql_cmd)
        except Exception as msg:
            print(f"Error occured while connecting to database => {str(msg)}")

    def load(self):
        """
        Reads each file from source directory and inserts the records into the database.

        Note:
            Before using this method, ensure that the files exist in the specified directory.

        Raises:
            FileNotFoundError: If the directory or file is not found.
            EmptyDataError: If there is error with the dataframe or the dataframe is empty.
            Exception: If there is an issue connecting to the database or executing the SQL queries.
        """

        col_name_list = self.__get_column_names()

        sql_insert: str = (
            "INSERT INTO " + DateLoader.__tbl_name +
            " (UNICOM_PARAM, DENUMERE_CLIENT, TIP_CLIENT, SEGMENT_EE, SEGMENT_GN, "
            "ETICHETA_REGIUNE, MACRO_ZONA_CLIENT, MACRO_DEPARTAMENT_GESTIONARE, "
            "TRANSA_EE, TRANSA_GN, ETICHETA_PORTOFOLIU, COD_UNICOM, NOM_UNICOM, "
            "COD_TAR, TARIF, DATA_INTRARE_VIGOARE, DATA_REZILIERE, DATA_ESTIMARE_REZILIERE, "
            "DATA_MODIFICARE, CML_CALCULAT, CML_CALCULAT_SECV_PREC, CLI_TRANSF_DIN_REGLEMENTAT, "
            "PERIOADA_VALABILTIATE_CONV, CONVENTIE_CONSUM_LUNA_1, CONVENTIE_CONSUM_LUNA_2, "
            "CONVENTIE_CONSUM_LUNA_3, CONVENTIE_CONSUM_LUNA_4, CONVENTIE_CONSUM_LUNA_5, "
            "CONVENTIE_CONSUM_LUNA_6, CONVENTIE_CONSUM_LUNA_7, CONVENTIE_CONSUM_LUNA_8, "
            "CONVENTIE_CONSUM_LUNA_9, CONVENTIE_CONSUM_LUNA_10, CONVENTIE_CONSUM_LUNA_11, "
            "CONVENTIE_CONSUM_LUNA_12, COD_CLI, NIS_RAD, SEC_NIS, EST_SUM, STARE_CONTRACT, "
            "PRET, PRET2, PRET_REZERVARE, CUI, PRET_TARIF_DISTRIBUTIE, VALOARE_DIAGARA_CONSUM_ZONA1, "
            "VALOARE_DIAGARA_CONSUM_ZONA2, PRET_INTRODUCERE_RETEA, PRET_EXTRAJERE_RETEA, PRET_TARIF_SISTEM, "
            "PRET_TRANSPORT_GAZ, DATA_ACTIVARE_FACT_ELECTR, POD, COD_DZ, ZONA_DISTRIBUTIE, CATEG, "
            "SURSA, AN, LUNA "
            ")"
            "VALUES ( :0, :1, :2, :3, :4, "
            ":5, :6, :7, "
            ":8, :9, :10, :11, :12, "
            ":13, :14, :15, :16, :17, "
            ":18, :19, :20, :21, "
            ":22, :23, :24, "
            ":25, :26, :27, "
            ":28, :29, :30, "
            ":31, :32, :33, "
            ":34, :35, :36, :37, :38, :39,  "
            ":40, :41, :42, :43, :44, :45,  "
            ":46, :47, :48, :49,   "
            ":50, :51, :52, :53, :54, :55,  "
            ":56, :57, :58 ) "
        )

        try:
            os.chdir(self.path_output)
            all_filenames = [i for i in glob.glob(f"*.{self.extension}")]
            for f in all_filenames:
                print(f"FILE {f} LOADED INTO DB !")

                src_int = pd.read_csv(
                    f,
                    sep="|",
                    header=0,
                    decimal=".",
                    thousands=",",
                    converters={
                        "UNICOM_PARAM": str,
                        "DENUMERE_CLIENT": str,
                        "TIP_CLIENT": str,
                        "SEGMENT_EE": str,
                        "SEGMENT_GN": str,
                        "ETICHETA_REGIUNE": str,
                        "MACRO_ZONA_CLIENT": str,
                        "MACRO_DEPARTAMENT_GESTIONARE": str,
                        "TRANSA_EE": str,
                        "TRANSA_GN": str,
                        "ETICHETA_PORTOFOLIU": str,
                        "COD_UNICOM": str,
                        "NOM_UNICOM": str,
                        "COD_TAR": str,
                        "TARIF": str,
                        "DATA_INTRARE_VIGOARE": str,
                        "DATA_REZILIERE": str,
                        "DATA_ESTIMARE_REZILIERE": str,
                        "DATA_MODIFICARE": str,
                        "CML_CALCULAT": float,
                        "CML_CALCULAT_SECV_PREC": float,
                        "CLI_TRANSF_DIN_REGLEMENTAT": str,
                        "PERIOADA_VALABILTIATE_CONV": str,
                        "CONVENTIE_CONSUM_LUNA_1": float,
                        "CONVENTIE_CONSUM_LUNA_2": float,
                        "CONVENTIE_CONSUM_LUNA_3": float,
                        "CONVENTIE_CONSUM_LUNA_4": float,
                        "CONVENTIE_CONSUM_LUNA_5": float,
                        "CONVENTIE_CONSUM_LUNA_6": float,
                        "CONVENTIE_CONSUM_LUNA_7": float,
                        "CONVENTIE_CONSUM_LUNA_8": float,
                        "CONVENTIE_CONSUM_LUNA_9": float,
                        "CONVENTIE_CONSUM_LUNA_10": float,
                        "CONVENTIE_CONSUM_LUNA_11": float,
                        "CONVENTIE_CONSUM_LUNA_12": float,
                        "COD_CLI": str,
                        "NIS_RAD": str,
                        "SEC_NIS": str,
                        "EST_SUM": str,
                        "STARE_CONTRACT": str,
                        "PRET": float,
                        "PRET2": float,
                        "PRET_REZERVARE": float,
                        "CUI": str,
                        "PRET_TARIF_DISTRIBUTIE": float,
                        "VALOARE_DIAGARA_CONSUM_ZONA1": float,
                        "VALOARE_DIAGARA_CONSUM_ZONA2": float,
                        "PRET_INTRODUCERE_RETEA": float,
                        "PRET_EXTRAJERE_RETEA": float,
                        "PRET_TARIF_SISTEM": float,
                        "PRET_TRANSPORT_GAZ": float,
                        "DATA_ACTIVARE_FACT_ELECTR": str,
                        "POD": str,
                        "COD_DZ": str,
                        "ZONA_DISTRIBUTIE": str,
                        "CATEG": str,
                        "SURSA": str,
                        "AN": int,
                        "LUNA": int
                    },
                    engine="python",
                    encoding="unicode_escape",
                )
                src_int = src_int[col_name_list]
                df = pd.DataFrame(src_int).fillna("")
                src_calc = [tuple(x) for x in df.values]

                with db.oracle_conn() as conn:
                    conn.executemany(sql_insert, src_calc)
        except FileNotFoundError as msg:
            print(f"Error flat file not found => {str(msg)}")
        except pd.errors.EmptyDataError as msg:
            print(f"Error pandas dataframe si empty => {str(msg)}")
        except Exception as msg:
            print(f"Database error occured while loading data => {str(msg)}")
