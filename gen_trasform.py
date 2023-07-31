import pandas as pd
from datetime import datetime


def double_quote(word: str) -> str:
    return '"{}"'.format(word)


def single_quote(word: str) -> str:
    return repr(str(word))


# define column's to be used from flat file's
sel_col: list[str] = [
    'unicom_param',
    'denumere_client',
    'tip_client',
    'segment_ee',
    'segment_gn',
    'eticheta_regiune',
    'macro_zona_client',
    'macro_departament_gestionare',
    'transa_ee',
    'transa_gn',
    'eticheta_portofoliu',
    'cod_unicom',
    'nom_unicom',
    'cod_tar',
    'tarif',
    'data_intrare_vigoare',
    'data_reziliere',
    'data_estimare_reziliere',
    'data_modificare',
    'cml_calculat',
    'cml_calculat_secv_prec',
    'cli_transf_din_reglementat',
    'perioada_valabiltiate_conv',
    'conventie_consum_luna_1',
    'conventie_consum_luna_2',
    'conventie_consum_luna_3',
    'conventie_consum_luna_4',
    'conventie_consum_luna_5',
    'conventie_consum_luna_6',
    'conventie_consum_luna_7',
    'conventie_consum_luna_8',
    'conventie_consum_luna_9',
    'conventie_consum_luna_10',
    'conventie_consum_luna_11',
    'conventie_consum_luna_12',
    'cod_cli',
    'nis_rad',
    'sec_nis',
    'est_sum',
    'stare_contract',
    'pret',
    'pret2',
    'pret_rezervare',
    'cui',
    'pret_tarif_distributie',
    'valoare_diagara_consum_zona1',
    'valoare_diagara_consum_zona2',
    'pret_introducere_retea',
    'pret_extrajere_retea',
    'pret_tarif_sistem',
    'pret_transport_gaz',
    'data_activare_fact_electr',
    'pod',
    'cod_dz',
    'zona_distributie',
    'categ'
]
