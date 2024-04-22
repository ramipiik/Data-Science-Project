# Purpose
SMEAR II is the world's largest biosphere-atmosphere field station. It is well known that forests are repeatedly experiencing stress, but the frequency and severity of stress, and its impact on air chemistry and physics processes has never been quantified at SMEAR II.

Purpose of the project is to use data analysis and ML to answer: (1) How frequent does the vegetation at SMEAR II experience stress, how long does the stress prevail and how severe is the level of stress. And (2) which impact does the observed stress (duration, type and severity) have on ecosystem scale VOC fluxes in SMEAR II.

# Instructions
## Quick start
1. Clone the repository.
1. Download the data files listed below from https://smear.avaa.csc.fi/download and store them to your local `data` folder. (The other source data files are included in the repository. These ones were excluded due to their large size):<small>
   - HYY_META.PAR_1997-01-01--9999-09-09.csv
   - HYY_META.Precipacc_2005-04-01--9999-09-09.csv
   - HYY_META.T168_1997-01-01--9999-09-09.csv
   - HYY_META.T672_1997-01-01--9999-09-09.csv </small>
1. Install the needed libraries. See section *Environment management* for instructions.
1. Open and run the notebook `main.ipynb`.

## Environment management
All dependencies are listed in the file `requirements.txt`.

**Setup**
1. Create a new local virtual environment (named 'venv'): `python3 -m venv venv`
1. Start the virtual environment: `source venv/bin/activate`
1. Exit the virtual environment: `deactivate`  

**Update your local environment based on changes in requirements.txt**
1. Activate your local virtual environment: `source venv/bin/activate`
1. Pull changes from github to your local computer
1. Update local environment: `pip install -r requirements.txt`

**Add your local changes to requirements.txt**
1. Double check that your local environment is up-to-date, i.e. that you are building on top of the latest common environment as defined in `requirements.txt` in github. 
1. Double check that you have activated the right virtual environment locally. This is to make sure that you are updating the dependencies from the desired local environment.   
1. Add the new dependencies using `pip` package manager.
1. Run `pip freeze > requirements.txt` in order to update `requirements.txt`.
1. Commit and push the changes to github.

## More about the repository
- Several configuration options are available and can be edited in the beginning of the `main.ipynb` notebook.
- Source data files are stored in the `data` folder. The folder includes a separate `readme.md` file with descriptions of the variables. 
- Theoretical NEE and monoterpene baselines can be found in the `baseline` folder. They are calculated with the notebook in the same folder.

# Data
The data used in this project consists of biosphere and atmosphere data from the SMEAR II station. Main time period used in the analysis is 2014-2022. Examples of variables are: NEE (net ecosystem exchange), VOC emissions, soil water content, temperature, light, cloudyness etc. All of the used data is open and can be checked out here: https://smear.avaa.csc.fi/download. 


