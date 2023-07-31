import datetime
import transform
import load


p_an_rap = int(input("Enter Year: "))
p_luna_rap = int(input("Enter Month : "))


if __name__ == "__main__":
    start_time = datetime.datetime.now()
    per_report: str = str(p_an_rap) + "-" + str(p_luna_rap)

    et_process = transform.DateTransformer(path_input="PATH_FOR_FILE_INPUT",
                                           path_input_clean="PATH_FOR_FILE_INPUT_CLEAN",
                                           path_output="PATH_FOR_FILE_OUTPUT",
                                           extension="csv",
                                           batch=100000)

    l_process = load.DateLoader(path_output="PATH_FOR_FILE_OUTPUT",
                                extension="csv", an=p_an_rap, luna=p_luna_rap)

    print(
        f"""
    ____________________________________________________________________________________________
    RUN ETL FOR PARAM: 
    REPORTED PERIOD : {per_report}
    ____________________________________________________________________________________________
    """
    )

    print("00 - START => EXTRACT AND TRANSFORM PROCESS ! ")

    et_process.extract()
    et_process.add_columns(p_an_rap, p_luna_rap)

    print("99 - FINISH => EXTRACT AND TRANSFORM PROCESS ! ")

    print(
        "____________________________________________________________________________________________"
    )
    print("00 - START => LOADING PROCESS ! ")

    l_process.delete_from_tbl()
    l_process.load()

    print("99 - FINISH => LOADING PROCESS ! ")

    end_time = datetime.datetime.now()
    run_time = end_time - start_time
    print(
        f"""
        ____________________________________________________________________________________________
        RUN ETL IN {run_time} SECONDS !
        ____________________________________________________________________________________________
        """
    )
