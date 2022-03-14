## Install Spark

```
brew install java
brew install apache-spark
```

Add the following environment variables to your `.bash_profile` or `.zshrc`:

```
export JAVA_HOME=/usr/local/Cellar/openjdk@11/11.0.12
export PATH="$JAVA_HOME/bin/:$PATH"
export SPARK_HOME=/usr/local/Cellar/apache-spark/3.2.1/libexec
export PATH="$SPARK_HOME/bin/:$PATH"
```

## Install PySpark

Run ``ls -la /usr/local/Cellar/apache-spark/3.2.1/libexec/python/lib/py4j*`` and copy the name of the py4j zip file. In my case it is ``py4j-0.10.9.3-src.zip``. Add this file to the PYTHONPATH below.

```
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.3-src.zip:$PYTHONPATH"
```

Create a conda environment ``spark`` for pyspark with python 3.8 (in my case 3.8 is arbitrary, pyspark needs at least python 3.6)

```
conda env create -f conda_environment.yml
conda activate spark
```

## Execute code

You can execute the Jupyter Notebooks in the `code` folder using Visual Studio Code.

*I did not install Jupyter in the environment, threrefore notebooks can not be executed using Jupyter Lab/Notebook.*

