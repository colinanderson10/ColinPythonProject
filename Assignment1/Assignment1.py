import sys

import pandas
import plotly.express as px
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

    print("\nDescriptive Statistics through Pandas\n", iris.describe())

    # Make Plots

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

    normalizer = Normalizer()

    normalizer.fit(X_orig)

    X = normalizer.transform(X_orig)

    random_forest = RandomForestClassifier(random_state=2222)
    random_forest.fit(X, y)

    neigh = KNeighborsClassifier(n_neighbors=4)
    neigh.fit(X, y)

    print("\nModel via Random Forest Pipeline Predictions")
    pipeline = Pipeline(
        [
            ("Normalizer", Normalizer()),
            ("RandomForest", random_forest),
        ]
    )
    pipeline.fit(X_orig, y)

    print("\nRandom Forest Probability:\n", pipeline.predict_proba(X))
    print("\nRandom Forest Score:", pipeline.score(X, y))

    print("\nModel via Nearest Neighbor Pipeline Predictions")
    pipeline = Pipeline(
        [
            ("Normalizer", Normalizer()),
            ("Nearest Neighbor", neigh),
        ]
    )
    pipeline.fit(X_orig, y)

    print("\nNearest Neighbor Probability:\n", pipeline.predict_proba(X))
    print("\nNearest Neighbor Score:", pipeline.score(X, y))
    return


if __name__ == "__main__":
    sys.exit(main())
