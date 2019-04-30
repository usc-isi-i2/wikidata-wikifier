# import os
# from .sparql import DEFAULT_SAVE_LOCATION, save_prop_idents
from .find_identity import FindIdentity
import typing
# import numpy as np


# class Wikifier:
#     def __init__(self, base_file_loc:str=None):
#         if base_file_loc:
#             pass
#         else:
#             print("No base file given, try to find the base file on default location")
#             if os.path.exists(DEFAULT_SAVE_LOCATION):
#                 pass
#                 # with open(DEFAULT_SAVE_LOCATION, "r")
#             else:
#                 print(" No base file found on default save location please run Wikifier.initialize() first")
#
#     @staticmethod
#     def initialize():
#         save_prop_idents()

def produce(inputs, target_columns: typing.List[int]=None, input_type: str="pandas"):
    if input_type == "pandas":
        return produce_for_pandas(input_df=inputs, target_columns=target_columns)
    # elif input_type == "d3m_ds":
    #     return produce_for_d3m_dataset(input_ds=inputs, target_columns=target_columns)
    # elif input_type == "d3m_df":
    #     return produce_for_d3m_dataframe(input_df=inputs, target_columns=target_columns)
    else:
        raise ValueError("unknown type of input!")


def produce_for_pandas(input_df, target_columns: typing.List[int]=None):
    """
    function used to produce for input type is pandas.dataFrame
    :param input_df: input pd.dataFrame
    :param target_columns: target columns to find with wikidata
    :return: a pd.dataFrame with updated columns from wikidata
    """
    # if no target columns given, just try every columns
    if target_columns is None:
        target_columns = list(range(input_df.shape[1]))

    return_df = input_df
    for i, column in enumerate(input_df.columns[target_columns]):
        curData = [str(x) for x in list(input_df[column])]
        # for each column, try to find corresponding possible P nodes id first
        for idx, res in enumerate(FindIdentity.get_identifier_3(curData, input_df.columns[i])):
            # res[0] is the send input P node
            top1_dict = res[1]
            new_col = [""] * len(curData)
            for i in range(len(curData)):
                if curData[i] in top1_dict:
                    new_col[i] = top1_dict[curData[i]]
            col_name = column + '_wikidata'
            return_df[col_name] = new_col
            break
    return return_df



