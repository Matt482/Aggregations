import pandas as pd
import re
from helpers import create_df, get_data_identifier_url, create_df_2


# TODO: finish all docstring for functions primary for this main file optionaly also for helpers
def get_transactions(identifier: str) -> pd.DataFrame:
    """
    fetch url and return pd dataframe with columns IDENTIFIER,
    TIME_PERIOD and OBS_VALUE corresponding to values of the identifier
    parameter generic:ObsDimension tag and generic:ObsValue
    :param identifier: can be replaced with any user-specified identifier
    """
    url = get_data_identifier_url(identifier)
    df = create_df(url, identifier)
    # construct = {"IDENTIFIER": identifier, "TIME_PERIOD": df['TIME_PERIOD'], "OBS_VALUE": df['OBS_VALUE']}
    # res = pd.DataFrame(construct)
    return df


def get_formula_data(formula: str) -> pd.DataFrame:
    """
    :param formula: left side of = contain only one identifier
    right side of = will only include addition (+) and substraction (-) operators
    number of identifiers on the right-hand side is not fixed.
    :return: df consists from all url identifiers from right side of =
    """
    splitted = formula.split('=')

    ####TODO: IMPLEMENT FUNCTIONALITY TO TAKE INPUT AS BETTER WAY
    parse_args = re.split('[+\-]', splitted[1].replace(' ', ''))
    # operators = [x for x in formula if x == '+' or x == '-']

    count = 0
    for data in parse_args:
        if count == 0:
            basic_url = f"https://sdw-wsrest.ecb.europa.eu/service/data/BP6/{data}?detail=dataonly"
            df = create_df_2(basic_url, data)
            count += 1
        else:
            basic_url = f"https://sdw-wsrest.ecb.europa.eu/service/data/BP6/{data}?detail=dataonly"
            sralo_2 = create_df_2(basic_url, data)
            df.insert(0, data, sralo_2[data])

    return df


def compute_aggregates(formula: str) -> pd.DataFrame:
    """
    depends on formula on the right side of = when + between dfs
    do concatenation if - do subtraction of OBS_VALUE columns
    :param formula: consists from multiple identifiers
    on the left side of = is column name and on the right side
    of = are dataframes which we are going to calculate
    :return: final df after mathematical operations only (+ or -)
    """
    formula_2 = formula.strip()
    column = formula_2.split('=')[0]

    result_2 = formula_2.split('=')[-1].replace(' ', '')
    parse_args = re.split('[+\-]', result_2)

    data_placeholder = []
    for data in parse_args:
        basic_url = f"https://sdw-wsrest.ecb.europa.eu/service/data/BP6/{data}?detail=dataonly"
        my_df = create_df(basic_url, data)
        my_df = my_df.set_index('TIME_PERIOD')
        data_placeholder.append(my_df)

    final_df = pd.DataFrame()
    for x in range(0, len(data_placeholder) - 1):

        for y in formula_2.split('=')[1]:
            try:
                if y == '+':
                    final_df[column] = pd.concat(
                        [data_placeholder[0]['OBS_VALUE'] + data_placeholder[0 + 1]['OBS_VALUE']])
                elif y == '-':
                    final_df[column] = pd.concat(
                        [data_placeholder[0]['OBS_VALUE'] - data_placeholder[0 + 1]['OBS_VALUE']])

            except:
                print('exception arrise')

    return final_df


# print(get_transactions("Q.N.I8.W1.S1.S1.T.A.FA.D.F._Z.EUR._T._X.N"))

# xa = get_formula_data("Q.N.I8.W1.S1.S1.T.A.FA.D.F._Z.EUR._T._X.N =Q.N.I8.W1.S1P.S1.T.A.FA.D.F._Z.EUR._T._X.N +Q.N.I8.W1.S1Q.S1.T.A.FA.D.F._Z.EUR._T._X.N")
# print(xa)

# xaxa = compute_aggregates("Q.N.I8.W1.S1.S1.T.A.FA.D.F._Z.EUR._T._X.N =Q.N.I8.W1.S1P.S1.T.A.FA.D.F._Z.EUR._T._X.N +Q.N.I8.W1.S1Q.S1.T.A.FA.D.F._Z.EUR._T._X.N")
# print(xaxa)
