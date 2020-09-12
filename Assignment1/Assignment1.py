import sys

import numpy
import pandas
import plotly.express as px
import plotly.io as pio
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import Normalizer


def main():
    # Load the whole csv into iris dataframe
    iris = pandas.read_csv("iris.data", header=None)

    # add column names
    iris.columns = [
        "Sepal_Length",
        "Sepal_Width",
        "Petal_Length",
        "Petal_Width",
        "Type",
    ]

    # Use pandas to return descriptive stats

    print("\nDescriptive Statistics through Pandas\n\n", iris.describe())

    # Use numpy to return descriptive stats

    print(
        "\nDescriptive Statistics through Numpy:\n\nVariable Means\n"
        + str(numpy.mean(iris))
    )

    print("\nVariable Minimums\n" + str(numpy.min(iris)))

    print("\nVariable Maximums\n" + str(numpy.max(iris)))

    irisSepalLength = iris.drop(
        columns=["Sepal_Width", "Petal_Length", "Petal_Width", "Type"]
    )

    irisSepalWidth = iris.drop(
        columns=["Sepal_Length", "Petal_Length", "Petal_Width", "Type"]
    )

    irisPetalLength = iris.drop(
        columns=["Sepal_Length", "Sepal_Width", "Petal_Width", "Type"]
    )

    irisPetalWidth = iris.drop(
        columns=["Sepal_Length", "Sepal_Width", "Petal_Length", "Type"]
    )

    print(
        "\nVariable 25% Quartiles\n"
        + "Sepal Length: "
        + str(numpy.quantile(irisSepalLength, 0.25, axis=0))
        + "\nSepal Width: "
        + str(numpy.quantile(irisSepalWidth, 0.25, axis=0))
        + "\nPetal Length: "
        + str(numpy.quantile(irisPetalLength, 0.25, axis=0))
        + "\nPetal Width: "
        + str(numpy.quantile(irisPetalWidth, 0.25, axis=0))
    )

    print(
        "\nVariable 75% Quartiles\n"
        + "Sepal Length: "
        + str(numpy.quantile(irisSepalLength, 0.75, axis=0))
        + "\nSepal Width: "
        + str(numpy.quantile(irisSepalWidth, 0.75, axis=0))
        + "\nPetal Length: "
        + str(numpy.quantile(irisPetalLength, 0.75, axis=0))
        + "\nPetal Width: "
        + str(numpy.quantile(irisPetalWidth, 0.75, axis=0))
    )

    # Make Plots

    pio.renderers.default = "browser"

    # Scatter Plot

    scatterplot = px.scatter(iris, x="Petal_Length", y="Petal_Width", color="Type")

    scatterplot.show()

    # Violin Plot

    violinplot = px.violin(iris, y="Petal_Length", color="Type")

    violinplot.show()

    # Histogram

    hist = px.histogram(iris, x="Petal_Length", color="Type")

    hist.show()

    # Box Plot

    box = px.box(iris, x="Petal_Length", y="Petal_Width", color="Type")

    box.show()

    # Marginal Distribution Plot

    fig = px.scatter(
        iris,
        x="Petal_Length",
        y="Petal_Width",
        marginal_x="histogram",
        marginal_y="histogram",
        color="Type",
    )

    fig.show()

    # Models using Scikitlearn

    X_orig = iris[["Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width"]].values

    y = iris["Type"].values

    print("\nModel via Random Forest Pipeline Predictions")
    pipeline1 = Pipeline(
        [
            ("Normalizer", Normalizer()),
            ("RandomForest", RandomForestClassifier(random_state=2222)),
        ]
    )
    pipeline1.fit(X_orig, y)

    print("\nRandom Forest Probability:\n", pipeline1.predict_proba(X_orig))
    print("\nRandom Forest Score:", pipeline1.score(X_orig, y))

    print("\nModel via Nearest Neighbor Pipeline Predictions")
    pipeline2 = Pipeline(
        [
            ("Normalizer", Normalizer()),
            ("Nearest Neighbor", KNeighborsClassifier(n_neighbors=4)),
        ]
    )
    pipeline2.fit(X_orig, y)

    print("\nNearest Neighbor Probability:\n", pipeline2.predict_proba(X_orig))
    print("\nNearest Neighbor Score:", pipeline2.score(X_orig, y))
    return


if __name__ == "__main__":
    sys.exit(main())
