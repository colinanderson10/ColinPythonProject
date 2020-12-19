import os
import sys
import warnings

import numpy
import pandas
import plotly.express
import plotly.subplots
import plotly.tools
import scipy
import sklearn.metrics


def main():
    path = os.getcwd()
    print(path)

    pandas.set_option("display.max_columns", None)

    # Input file location of csv in LOCATION and COLUMN NAMES under COLUMN_NAMES
    """
    data = pandas.read_csv("LOCATION", header=None)
    column_names = [
        "Column1",
        "Column2",
    ]
    data.columns = column_names

    for y in column_names:
        e = data[y].to_frame()
        if e.dtypes[0] == object:
            data = pandas.get_dummies(data, dtype=int)

    columns = data.columns.values.tolist()
    print("Column names:\n", columns)

    """
    # Input file location of csv in LOCATION and COLUMN NAMES under COLUMN_NAMES
    data = pandas.read_csv("iris.data", header=None)
    data_column_names = [
        "sepal_length",
        "sepal_width",
        "petal_length",
        "petal_width",
        "binomial",
        "binomial2",
        "bool",
        "class",
    ]
    data.columns = data_column_names

    for y in data_column_names:
        e = data[y].to_frame()
        if e.dtypes[0] == object:
            data = pandas.get_dummies(data, dtype=int)

    columns = data.columns.values.tolist()

    print("Column names:\n", columns)

    def response_entry():
        response = input("\nEnter response variable:\n")
        if response in columns:
            return response
        else:
            print("The indicated variable is not in the dataset.")
            return response_entry("response")

    response = response_entry()

    response_data = data[response].to_frame()

    """
    if response_data.dtypes[0] == bool:
        response_type = "Boolean"
        print("The Response variable, " + response + ", is a Boolean Variable")
        # encode boolean to categorical variable True=1 False=0
        for value in response_data.values:
            if value is True:
                value = 1
            else:
                value = 0

    else:
        response_type = "Continuous"
        print("The Response variable, " + response + ", is a Continuous Variable")
    """

    resp_plot = plotly.express.histogram(response_data)

    response_html = response.replace(" ", "")

    resp_plot.write_html(file=f"{response_html}_histogram.html", include_plotlyjs="cdn")

    predictor_data = data.drop(columns=response)

    predictor_columns = list(predictor_data.columns)

    response_mean = response_data.mean()

    contbins = int(input(f"\nEnter number of bins for Continuous Variables:\n"))

    n = data.shape[0]

    # Split predictors into separate dataframes
    Continuous_Predictors = pandas.DataFrame()

    Categorical_Predictors = pandas.DataFrame()

    for predictor in predictor_columns:

        c = data[predictor].to_frame()
        print(c)
        print(c.dtypes[0])
        if c.dtypes[0] == float:
            Continuous_Predictors[predictor] = data[predictor]
        else:
            Categorical_Predictors[predictor] = data[predictor]

    print(Continuous_Predictors)

    Continuous_Predictors_Names = Continuous_Predictors.columns.values.tolist()

    print(Continuous_Predictors_Names)

    print(Categorical_Predictors)

    Categorical_Predictors_Names = Categorical_Predictors.columns.values.tolist()

    print(Categorical_Predictors_Names)

    # create continuous-continuous pairs

    column_names = [
        "Predictor1",
        "Predictor2",
    ]

    Cont_Cont_Pairs = pandas.DataFrame(columns=column_names)

    Cont_Cat_Pairs = pandas.DataFrame(columns=column_names)

    Cat_Cat_Pairs = pandas.DataFrame(columns=column_names)

    i = 0
    for counter1, name1 in enumerate(Continuous_Predictors_Names):
        for counter2, name2 in enumerate(Continuous_Predictors_Names):
            if counter1 < counter2:
                Cont_Cont_Pairs.loc[i] = [name1, name2]
                i += 1

    print(Cont_Cont_Pairs)

    i = 0
    for name1 in Continuous_Predictors_Names:
        for name2 in Categorical_Predictors_Names:
            Cont_Cat_Pairs.loc[i] = [name1, name2]
            i += 1

    print(Cont_Cat_Pairs)

    i = 0
    for counter1, name1 in enumerate(Categorical_Predictors_Names):
        for counter2, name2 in enumerate(Categorical_Predictors_Names):
            if counter1 < counter2:
                Cat_Cat_Pairs.loc[i] = [name1, name2]
                i += 1

    print(Cat_Cat_Pairs)

    # Create Correlation values and plots between the continuous-continuous pairs
    corrlist = []
    plotlist = []
    for index, row in Cont_Cont_Pairs.iterrows():
        predictor1 = row["Predictor1"]
        predictor2 = row["Predictor2"]
        correlationvalue = scipy.stats.pearsonr(data[predictor1], data[predictor2])
        corrlist.append(correlationvalue[0])

        figure1 = plotly.express.scatter(
            x=data[predictor1],
            y=data[predictor2],
            trendline="ols",
        )
        figure1.update_layout(
            title="Correlation between two Continuous Predictors",
            xaxis_title=predictor1,
            yaxis_title=predictor2,
        )

        predictor1_html = predictor1.replace(" ", "")
        predictor2_html = predictor2.replace(" ", "")
        relate_file_save = (
            f"{path}/{predictor1_html}_{predictor2_html}_scatter_plot.html"
        )
        relate_file_link = (
            "<a target='blank' href="
            + relate_file_save
            + ">"
            + relate_file_save
            + "</a>"
        )
        figure1.write_html(file=relate_file_save, include_plotlyjs="cdn")
        plotlist.append(relate_file_link)

    Cont_Cont_Correlation_Table = Cont_Cont_Pairs

    Cont_Cont_Correlation_Table["Correlation_Metric"] = corrlist
    Cont_Cont_Correlation_Table["Relationship_Plot"] = plotlist

    Cont_Cont_Correlation_Table_Final = Cont_Cont_Correlation_Table.sort_values(
        ["Correlation_Metric"], ascending=False
    )

    Cont_Cont_Correlation_Table_Final.to_html(
        "Continuous_Continuous_Correlation_Table.html", escape=False
    )

    # Create Continiuous-Continuous correlation matrix

    Cont_Cont_Matrix = pandas.DataFrame(
        columns=Continuous_Predictors_Names, index=Continuous_Predictors_Names
    )

    for x in Continuous_Predictors_Names:
        corrs = []
        for y in Continuous_Predictors_Names:
            correlationvalue = scipy.stats.pearsonr(data[x], data[y])
            corrs.append(correlationvalue[0])
        Cont_Cont_Matrix[x] = corrs

    Cont_Cont_Matrix.to_html(
        "Continuous_Continuous_Correlation_Matrix.html", escape=False
    )

    # Create Continiuous-Continuous brute force table

    Cont_Cont_Brute_Force_Table = pandas.DataFrame()

    DifferenceWithMeanOfResponse = []
    DifferenceWithMeanOfResponseWeighted = []
    DifferenceWithMeanOfResponseHist = []
    DifferenceWithMeanOfResponseValuesPlot = []
    for index, row in Cont_Cont_Pairs.iterrows():
        Cont_Cont_Brute_Force_Counts_Plot = pandas.DataFrame()
        Cont_Cont_Brute_Force_Values_Plot = pandas.DataFrame()
        predictor1 = row["Predictor1"]
        predictor2 = row["Predictor2"]
        min1 = data[predictor1].min()
        min2 = data[predictor2].min()
        max1 = data[predictor1].max()
        max2 = data[predictor2].max()
        DMR = 0
        DMRW = 0
        binmins1 = []
        binmins2 = []
        range1 = max1 - min1
        range2 = max2 - min2
        binsize1 = range1 / contbins
        binsize2 = range2 / contbins
        bins1 = []
        bins2 = []
        i = 0
        a1 = min1
        b1 = min1 + binsize1
        a2 = min2
        b2 = min2 + binsize2
        while i < contbins:
            binmins1.append(a1)
            bins1.append([a1, b1])
            a1 += binsize1
            b1 += binsize1
            binmins2.append(a2)
            bins2.append([a2, b2])
            a2 += binsize2
            b2 += binsize2
            i += 1
        for x in bins1:
            BinCounts = []
            MeanSquareDiff = []
            bindata = data.loc[data[predictor1].between(x[0], x[1], inclusive=True)]
            for y in bins2:
                Count1 = bindata.loc[
                    bindata[predictor2].between(y[0], y[1], inclusive=True),
                    response,
                ].count()
                BinCounts.append(Count1)
                LocalMean = bindata.loc[
                    bindata[predictor2].between(y[0], y[1], inclusive=True),
                    response,
                ].mean()
                PopulationMean = response_mean[0]
                MeanSquareDiff.append((LocalMean - PopulationMean) ** 2)
                if Count1 > 0:
                    DMR += (LocalMean - PopulationMean) ** 2
                    DMRW += ((LocalMean - PopulationMean) ** 2) * (Count1 / n)
            f = str(x[0])
            Cont_Cont_Brute_Force_Counts_Plot[f] = BinCounts
            Cont_Cont_Brute_Force_Values_Plot[f] = MeanSquareDiff

        DifferenceWithMeanOfResponse.append(DMR / n)

        DifferenceWithMeanOfResponseWeighted.append(DMRW)

        Cont_Cont_Brute_Force_Counts_Plot["index"] = binmins2
        Cont_Cont_Brute_Force_Counts_Plot.set_index("index", inplace=True)
        Cont_Cont_Brute_Force_Counts_Plot.index.name = None

        predictor1_html = predictor1.replace(" ", "")
        predictor2_html = predictor2.replace(" ", "")
        relate_file_save = (
            f"{path}/{predictor1_html}_{predictor2_html}_Mean_Response_Counts.html"
        )
        relate_file_link = (
            "<a target='blank' href="
            + relate_file_save
            + ">"
            + relate_file_save
            + "</a>"
        )

        Cont_Cont_Brute_Force_Counts_Plot.to_html(relate_file_save, escape=False)
        DifferenceWithMeanOfResponseHist.append(relate_file_link)

        Cont_Cont_Brute_Force_Values_Plot["index"] = binmins2
        Cont_Cont_Brute_Force_Values_Plot.set_index("index", inplace=True, drop=True)
        Cont_Cont_Brute_Force_Values_Plot.index.name = None

        relate_file_save = (
            f"{path}/{predictor1_html}_{predictor2_html}_Mean_Response_Values.html"
        )
        relate_file_link = (
            "<a target='blank' href="
            + relate_file_save
            + ">"
            + relate_file_save
            + "</a>"
        )

        Cont_Cont_Brute_Force_Values_Plot.to_html(relate_file_save, escape=False)
        DifferenceWithMeanOfResponseValuesPlot.append(relate_file_link)
    responses = []
    for p in Cont_Cont_Pairs["Predictor1"]:
        responses.append(str(response))

    Cont_Cont_Brute_Force_Table["Response"] = responses
    Cont_Cont_Brute_Force_Table["Predictor1"] = Cont_Cont_Pairs["Predictor1"]
    Cont_Cont_Brute_Force_Table["Predictor2"] = Cont_Cont_Pairs["Predictor2"]
    Cont_Cont_Brute_Force_Table[
        "DifferenceWithMeanOfResponse"
    ] = DifferenceWithMeanOfResponse
    Cont_Cont_Brute_Force_Table[
        "DifferenceWithMeanOfResponseWeighted"
    ] = DifferenceWithMeanOfResponseWeighted
    Cont_Cont_Brute_Force_Table[
        "DifferenceWithMeanOfResponseHist"
    ] = DifferenceWithMeanOfResponseHist
    Cont_Cont_Brute_Force_Table[
        "DifferenceWithMeanOfResponseValuesPlot"
    ] = DifferenceWithMeanOfResponseValuesPlot

    Cont_Cont_Brute_Force_Table_Final = Cont_Cont_Brute_Force_Table.sort_values(
        ["DifferenceWithMeanOfResponseWeighted"], ascending=False
    )

    Cont_Cont_Brute_Force_Table_Final.to_html(
        f"Continuous_Continuous_Mean_Response_Values.html", escape=False
    )

    # Create Correlation values and plots between the continuous-categorical pairs

    def cat_cont_correlation_ratio(categories, values):
        """
        Correlation Ratio: https://en.wikipedia.org/wiki/Correlation_ratio
        SOURCE:
        1.) https://towardsdatascience.com/the-search-for-categorical-correlation-a1cf7f1888c9
        :param categories: Numpy array of categories
        :param values: Numpy array of values
        :return: correlation
        """
        f_cat, _ = pandas.factorize(categories)
        cat_num = numpy.max(f_cat) + 1
        y_avg_array = numpy.zeros(cat_num)
        n_array = numpy.zeros(cat_num)
        for i in range(0, cat_num):
            cat_measures = values[numpy.argwhere(f_cat == i).flatten()]
            n_array[i] = len(cat_measures)
            y_avg_array[i] = numpy.average(cat_measures)
        y_total_avg = numpy.sum(numpy.multiply(y_avg_array, n_array)) / numpy.sum(
            n_array
        )
        numerator = numpy.sum(
            numpy.multiply(
                n_array, numpy.power(numpy.subtract(y_avg_array, y_total_avg), 2)
            )
        )
        denominator = numpy.sum(numpy.power(numpy.subtract(values, y_total_avg), 2))
        if numerator == 0:
            eta = 0.0
        else:
            eta = numpy.sqrt(numerator / denominator)
        return eta

    corrlist = []
    plotlist1 = []
    plotlist2 = []
    binsize = 0.3
    for index, row in Cont_Cat_Pairs.iterrows():
        predictor1 = row["Predictor1"]
        predictor2 = row["Predictor2"]
        correlationvalue = cat_cont_correlation_ratio(
            data[predictor2], data[predictor1]
        )
        corrlist.append(correlationvalue)

        catvalues = data[predictor2].unique().tolist()

        hist_data = []
        group_labels = []

        for r in catvalues:
            hist_data.append(data[predictor1].groupby(data[predictor2]).apply(list)[r])
            group_labels.append("Response = " + str(r))

        print(predictor1)
        print(predictor2)
        print(hist_data)
        print(group_labels)

        DistributionPlot = plotly.tools.FigureFactory.create_distplot(
            hist_data, group_labels, bin_size=binsize
        )
        DistributionPlot.update_layout(
            title="Continuous Predictor by Categorical Predictor",
            xaxis_title=predictor2,
            yaxis_title="Distribution",
        )

        predictor1_html = predictor1.replace(" ", "")
        predictor2_html = predictor2.replace(" ", "")
        relate_file_save = (
            f"{path}/{predictor1_html}_{predictor2_html}_distribution_plot.html"
        )
        relate_file_link = (
            "<a target='blank' href="
            + relate_file_save
            + ">"
            + relate_file_save
            + "</a>"
        )
        DistributionPlot.write_html(file=relate_file_save, include_plotlyjs="cdn")
        plotlist1.append(relate_file_link)

        figure1 = plotly.graph_objs.Figure()
        for curr_hist, curr_group in zip(hist_data, group_labels):
            figure1.add_trace(
                plotly.graph_objs.Violin(
                    x=numpy.repeat(curr_group, n),
                    y=curr_hist,
                    name=curr_group,
                    box_visible=True,
                    meanline_visible=True,
                )
            )
        figure1.update_layout(
            title="Continuous Predictor by Categorical Predictor",
            xaxis_title=predictor1,
            yaxis_title=predictor2,
        )

        relate_file_save = (
            f"{path}/{predictor1_html}_{predictor2_html}_violin_plot.html"
        )
        relate_file_link = (
            "<a target='blank' href="
            + relate_file_save
            + ">"
            + relate_file_save
            + "</a>"
        )
        figure1.write_html(file=relate_file_save, include_plotlyjs="cdn")
        plotlist2.append(relate_file_link)

    Cont_Cat_Correlation_Table = Cont_Cat_Pairs

    Cont_Cat_Correlation_Table["Correlation_Metric"] = corrlist
    Cont_Cat_Correlation_Table["Distribution_Plot"] = plotlist1
    Cont_Cat_Correlation_Table["Violin_Plot"] = plotlist2

    Cont_Cat_Correlation_Table_Final = Cont_Cat_Correlation_Table.sort_values(
        ["Correlation_Metric"], ascending=False
    )

    Cont_Cat_Correlation_Table_Final.to_html(
        "Continuous_Categorical_Correlation_Table.html", escape=False
    )

    # Create Continiuous-Categorical correlation matrix

    Cont_Cat_Matrix = pandas.DataFrame(
        columns=Categorical_Predictors_Names, index=Continuous_Predictors_Names
    )

    for x in Categorical_Predictors_Names:
        corrs = []
        for y in Continuous_Predictors_Names:
            correlationvalue = cat_cont_correlation_ratio(data[x], data[y])
            corrs.append(correlationvalue)
        Cont_Cat_Matrix[x] = corrs

    Cont_Cat_Matrix.to_html(
        "Continuous_Categorical_Correlation_Matrix.html", escape=False
    )

    # Create Continuous-Categorical brute force table

    Cont_Cat_Brute_Force_Table = pandas.DataFrame()

    DifferenceWithMeanOfResponse = []
    DifferenceWithMeanOfResponseWeighted = []
    DifferenceWithMeanOfResponseHist = []
    DifferenceWithMeanOfResponseValuesPlot = []
    for index, row in Cont_Cat_Pairs.iterrows():
        Cont_Cat_Brute_Force_Counts_Plot = pandas.DataFrame()
        Cont_Cat_Brute_Force_Values_Plot = pandas.DataFrame()
        predictor1 = row["Predictor1"]
        predictor2 = row["Predictor2"]
        catbins2 = data[predictor2].nunique()
        min1 = data[predictor1].min()
        min2 = data[predictor2].min()
        max1 = data[predictor1].max()
        max2 = data[predictor2].max()
        DMR = 0
        DMRW = 0
        binmins1 = []
        binmins2 = []
        range1 = max1 - min1
        range2 = max2 - min2
        binsize1 = range1 / contbins
        binsize2 = range2 / catbins2
        bins1 = []
        bins2 = []
        i = 0
        u = 0
        a1 = min1
        b1 = min1 + binsize1
        a2 = min2
        b2 = min2 + binsize2
        while i < contbins:
            binmins1.append(a1)
            bins1.append([a1, b1])
            a1 += binsize1
            b1 += binsize1
            i += 1
        while u < catbins2:
            binmins2.append(a2)
            bins2.append([a2, b2])
            a2 += binsize2
            b2 += binsize2
            u += 1
        for x in bins1:
            BinCounts = []
            MeanSquareDiff = []
            bindata = data.loc[data[predictor1].between(x[0], x[1], inclusive=True)]
            for y in bins2:
                Count1 = bindata.loc[
                    bindata[predictor2].between(y[0], y[1], inclusive=True),
                    response,
                ].count()
                BinCounts.append(Count1)
                LocalMean = bindata.loc[
                    bindata[predictor2].between(y[0], y[1], inclusive=True),
                    response,
                ].mean()
                PopulationMean = response_mean[0]
                MeanSquareDiff.append((LocalMean - PopulationMean) ** 2)
                if Count1 > 0:
                    DMR += (LocalMean - PopulationMean) ** 2
                    DMRW += ((LocalMean - PopulationMean) ** 2) * (Count1 / n)
            f = str(x[0])
            Cont_Cat_Brute_Force_Counts_Plot[f] = BinCounts
            Cont_Cat_Brute_Force_Values_Plot[f] = MeanSquareDiff

        DifferenceWithMeanOfResponse.append(DMR / n)

        DifferenceWithMeanOfResponseWeighted.append(DMRW)

        Cont_Cat_Brute_Force_Counts_Plot["index"] = binmins2
        Cont_Cat_Brute_Force_Counts_Plot.set_index("index", inplace=True)
        Cont_Cat_Brute_Force_Counts_Plot.index.name = None

        predictor1_html = predictor1.replace(" ", "")
        predictor2_html = predictor2.replace(" ", "")
        relate_file_save = (
            f"{path}/{predictor1_html}_{predictor2_html}_Mean_Response_Counts.html"
        )
        relate_file_link = (
            "<a target='blank' href="
            + relate_file_save
            + ">"
            + relate_file_save
            + "</a>"
        )

        Cont_Cat_Brute_Force_Counts_Plot.to_html(relate_file_save, escape=False)
        DifferenceWithMeanOfResponseHist.append(relate_file_link)

        Cont_Cat_Brute_Force_Values_Plot["index"] = binmins2
        Cont_Cat_Brute_Force_Values_Plot.set_index("index", inplace=True, drop=True)
        Cont_Cat_Brute_Force_Values_Plot.index.name = None

        relate_file_save = (
            f"{path}/{predictor1_html}_{predictor2_html}_Mean_Response_Values.html"
        )
        relate_file_link = (
            "<a target='blank' href="
            + relate_file_save
            + ">"
            + relate_file_save
            + "</a>"
        )

        Cont_Cat_Brute_Force_Values_Plot.to_html(relate_file_save, escape=False)
        DifferenceWithMeanOfResponseValuesPlot.append(relate_file_link)
    responses = []
    for p in Cont_Cat_Pairs["Predictor1"]:
        responses.append(str(response))

    Cont_Cat_Brute_Force_Table["Response"] = responses
    Cont_Cat_Brute_Force_Table["Predictor1"] = Cont_Cat_Pairs["Predictor1"]
    Cont_Cat_Brute_Force_Table["Predictor2"] = Cont_Cat_Pairs["Predictor2"]
    Cont_Cat_Brute_Force_Table[
        "DifferenceWithMeanOfResponse"
    ] = DifferenceWithMeanOfResponse
    Cont_Cat_Brute_Force_Table[
        "DifferenceWithMeanOfResponseWeighted"
    ] = DifferenceWithMeanOfResponseWeighted
    Cont_Cat_Brute_Force_Table[
        "DifferenceWithMeanOfResponseHist"
    ] = DifferenceWithMeanOfResponseHist
    Cont_Cat_Brute_Force_Table[
        "DifferenceWithMeanOfResponseValuesPlot"
    ] = DifferenceWithMeanOfResponseValuesPlot

    Cont_Cat_Brute_Force_Table_Final = Cont_Cat_Brute_Force_Table.sort_values(
        ["DifferenceWithMeanOfResponseWeighted"], ascending=False
    )

    Cont_Cat_Brute_Force_Table_Final.to_html(
        f"Continuous_Categorical_Mean_Response_Values.html", escape=False
    )

    # Create Correlation values and plots between the categorical-categorical pairs

    def fill_na(data):
        if isinstance(data, pandas.Series):
            return data.fillna(0)
        else:
            return numpy.array([value if value is not None else 0 for value in data])

    def cat_correlation(x, y, bias_correction=True, tschuprow=False):
        """
        Calculates correlation statistic for categorical-categorical association.
        The two measures supported are:
        1. Cramer'V ( default )
        2. Tschuprow'T

        SOURCES:
        1.) CODE: https://github.com/MavericksDS/pycorr
        2.) Used logic from:
            https://stackoverflow.com/questions/20892799/using-pandas-calculate-cram%C3%A9rs-coefficient-matrix
            to ignore yates correction factor on 2x2
        3.) Haven't validated Tschuprow

        Bias correction and formula's taken from:
        https://www.researchgate.net/publication/270277061_A_bias-correction_for_Cramer's_V_and_Tschuprow's_T

        Wikipedia for Cramer's V: https://en.wikipedia.org/wiki/Cram%C3%A9r%27s_V
        Wikipedia for Tschuprow' T: https://en.wikipedia.org/wiki/Tschuprow%27s_T
        Parameters:
        -----------
        x : list / ndarray / Pandas Series
            A sequence of categorical measurements
        y : list / NumPy ndarray / Pandas Series
            A sequence of categorical measurements
        bias_correction : Boolean, default = True
        tschuprow : Boolean, default = False
                   For choosing Tschuprow as measure
        Returns:
        --------
        float in the range of [0,1]
        """
        corr_coeff = numpy.nan
        try:
            x, y = fill_na(x), fill_na(y)
            crosstab_matrix = pandas.crosstab(x, y)
            n_observations = crosstab_matrix.sum().sum()

            yates_correct = True
            if bias_correction:
                if crosstab_matrix.shape == (2, 2):
                    yates_correct = False

            chi2, _, _, _ = scipy.stats.chi2_contingency(
                crosstab_matrix, correction=yates_correct
            )
            phi2 = chi2 / n_observations

            # r and c are number of categories of x and y
            r, c = crosstab_matrix.shape
            if bias_correction:
                phi2_corrected = max(
                    0, phi2 - ((r - 1) * (c - 1)) / (n_observations - 1)
                )
                r_corrected = r - ((r - 1) ** 2) / (n_observations - 1)
                c_corrected = c - ((c - 1) ** 2) / (n_observations - 1)
                if tschuprow:
                    corr_coeff = numpy.sqrt(
                        phi2_corrected
                        / numpy.sqrt((r_corrected - 1) * (c_corrected - 1))
                    )
                    return corr_coeff
                corr_coeff = numpy.sqrt(
                    phi2_corrected / min((r_corrected - 1), (c_corrected - 1))
                )
                return corr_coeff
            if tschuprow:
                corr_coeff = numpy.sqrt(phi2 / numpy.sqrt((r - 1) * (c - 1)))
                return corr_coeff
            corr_coeff = numpy.sqrt(phi2 / min((r - 1), (c - 1)))
            return corr_coeff
        except Exception as ex:
            print(ex)
            if tschuprow:
                warnings.warn("Error calculating Tschuprow's T", RuntimeWarning)
            else:
                warnings.warn("Error calculating Cramer's V", RuntimeWarning)
            return corr_coeff

    corrlist = []
    plotlist = []
    for index, row in Cat_Cat_Pairs.iterrows():
        predictor1 = row["Predictor1"]
        predictor2 = row["Predictor2"]
        correlationvalue = cat_correlation(data[predictor1], data[predictor2])
        corrlist.append(correlationvalue)

        conf_matrix = sklearn.metrics.confusion_matrix(
            data[predictor1], data[predictor2]
        )

        figure1 = plotly.graph_objs.Figure(
            data=plotly.graph_objs.Heatmap(
                z=conf_matrix, zmin=0, zmax=conf_matrix.max()
            )
        )
        figure1.update_layout(
            title="Categorical Predictor by Categorical Predictor (without relationship)",
            xaxis_title=predictor1,
            yaxis_title=predictor2,
        )

        predictor1_html = predictor1.replace(" ", "")
        predictor2_html = predictor2.replace(" ", "")
        relate_file_save = f"{path}/{predictor1_html}_{predictor2_html}_heat_map.html"
        relate_file_link = (
            "<a target='blank' href="
            + relate_file_save
            + ">"
            + relate_file_save
            + "</a>"
        )
        figure1.write_html(file=relate_file_save, include_plotlyjs="cdn")
        plotlist.append(relate_file_link)

    Cat_Cat_Correlation_Table = Cat_Cat_Pairs

    Cat_Cat_Correlation_Table["Correlation_Metric"] = corrlist
    Cat_Cat_Correlation_Table["Relationship_Plot"] = plotlist

    Cat_Cat_Correlation_Table_Final = Cat_Cat_Correlation_Table.sort_values(
        ["Correlation_Metric"], ascending=False
    )

    Cat_Cat_Correlation_Table_Final.to_html(
        "Categorical_Categorical_Correlation_Table.html", escape=False
    )

    # Create Categorical-Categorical correlation matrix

    Cat_Cat_Matrix = pandas.DataFrame(
        columns=Categorical_Predictors_Names, index=Categorical_Predictors_Names
    )

    for x in Categorical_Predictors_Names:
        corrs = []
        for y in Categorical_Predictors_Names:
            correlationvalue = cat_correlation(data[x], data[y])
            corrs.append(correlationvalue)
        Cat_Cat_Matrix[x] = corrs

    Cat_Cat_Matrix.to_html(
        "Categorical_Categorical_Correlation_Matrix.html", escape=False
    )

    # Create Categorical-Categorical brute force table

    Cat_Cat_Brute_Force_Table = pandas.DataFrame()

    DifferenceWithMeanOfResponse = []
    DifferenceWithMeanOfResponseWeighted = []
    DifferenceWithMeanOfResponseHist = []
    DifferenceWithMeanOfResponseValuesPlot = []
    for index, row in Cat_Cat_Pairs.iterrows():
        Cat_Cat_Brute_Force_Counts_Plot = pandas.DataFrame()
        Cat_Cat_Brute_Force_Values_Plot = pandas.DataFrame()
        predictor1 = row["Predictor1"]
        predictor2 = row["Predictor2"]
        catbins1 = data[predictor1].nunique()
        catbins2 = data[predictor2].nunique()
        min1 = data[predictor1].min()
        min2 = data[predictor2].min()
        max1 = data[predictor1].max()
        max2 = data[predictor2].max()
        DMR = 0
        DMRW = 0
        binmins1 = []
        binmins2 = []
        range1 = max1 - min1
        range2 = max2 - min2
        binsize1 = range1 / catbins1
        binsize2 = range2 / catbins2
        bins1 = []
        bins2 = []
        i = 0
        u = 0
        a1 = min1
        b1 = min1 + binsize1
        a2 = min2
        b2 = min2 + binsize2
        while i < catbins1:
            binmins1.append(a1)
            bins1.append([a1, b1])
            a1 += binsize1
            b1 += binsize1
            i += 1
        while u < catbins2:
            binmins2.append(a2)
            bins2.append([a2, b2])
            a2 += binsize2
            b2 += binsize2
            u += 1
        for x in bins1:
            BinCounts = []
            MeanSquareDiff = []
            bindata = data.loc[data[predictor1].between(x[0], x[1], inclusive=True)]
            for y in bins2:
                Count1 = bindata.loc[
                    bindata[predictor2].between(y[0], y[1], inclusive=True),
                    response,
                ].count()
                BinCounts.append(Count1)
                LocalMean = bindata.loc[
                    bindata[predictor2].between(y[0], y[1], inclusive=True),
                    response,
                ].mean()
                PopulationMean = response_mean[0]
                MeanSquareDiff.append((LocalMean - PopulationMean) ** 2)
                if Count1 > 0:
                    DMR += (LocalMean - PopulationMean) ** 2
                    DMRW += ((LocalMean - PopulationMean) ** 2) * (Count1 / n)
            f = str(x[0])
            Cat_Cat_Brute_Force_Counts_Plot[f] = BinCounts
            Cat_Cat_Brute_Force_Values_Plot[f] = MeanSquareDiff

        DifferenceWithMeanOfResponse.append(DMR / n)

        DifferenceWithMeanOfResponseWeighted.append(DMRW)

        Cat_Cat_Brute_Force_Counts_Plot["index"] = binmins2
        Cat_Cat_Brute_Force_Counts_Plot.set_index("index", inplace=True)
        Cat_Cat_Brute_Force_Counts_Plot.index.name = None

        predictor1_html = predictor1.replace(" ", "")
        predictor2_html = predictor2.replace(" ", "")
        relate_file_save = (
            f"{path}/{predictor1_html}_{predictor2_html}_Mean_Response_Counts.html"
        )
        relate_file_link = (
            "<a target='blank' href="
            + relate_file_save
            + ">"
            + relate_file_save
            + "</a>"
        )

        Cat_Cat_Brute_Force_Counts_Plot.to_html(relate_file_save, escape=False)
        DifferenceWithMeanOfResponseHist.append(relate_file_link)

        Cat_Cat_Brute_Force_Values_Plot["index"] = binmins2
        Cat_Cat_Brute_Force_Values_Plot.set_index("index", inplace=True, drop=True)
        Cat_Cat_Brute_Force_Values_Plot.index.name = None

        relate_file_save = (
            f"{path}/{predictor1_html}_{predictor2_html}_Mean_Response_Values.html"
        )
        relate_file_link = (
            "<a target='blank' href="
            + relate_file_save
            + ">"
            + relate_file_save
            + "</a>"
        )

        Cat_Cat_Brute_Force_Values_Plot.to_html(relate_file_save, escape=False)
        DifferenceWithMeanOfResponseValuesPlot.append(relate_file_link)

    responses = []
    for p in Cat_Cat_Pairs["Predictor1"]:
        responses.append(str(response))

    Cat_Cat_Brute_Force_Table["Response"] = responses
    Cat_Cat_Brute_Force_Table["Predictor1"] = Cat_Cat_Pairs["Predictor1"]
    Cat_Cat_Brute_Force_Table["Predictor2"] = Cat_Cat_Pairs["Predictor2"]
    Cat_Cat_Brute_Force_Table[
        "DifferenceWithMeanOfResponse"
    ] = DifferenceWithMeanOfResponse
    Cat_Cat_Brute_Force_Table[
        "DifferenceWithMeanOfResponseWeighted"
    ] = DifferenceWithMeanOfResponseWeighted
    Cat_Cat_Brute_Force_Table[
        "DifferenceWithMeanOfResponseHist"
    ] = DifferenceWithMeanOfResponseHist
    Cat_Cat_Brute_Force_Table[
        "DifferenceWithMeanOfResponseValuesPlot"
    ] = DifferenceWithMeanOfResponseValuesPlot

    Cat_Cat_Brute_Force_Table_Final = Cat_Cat_Brute_Force_Table.sort_values(
        ["DifferenceWithMeanOfResponseWeighted"], ascending=False
    )

    Cat_Cat_Brute_Force_Table_Final.to_html(
        f"Categorical_Categorical_Mean_Response_Values.html", escape=False
    )


if __name__ == "__main__":
    sys.exit(main())
