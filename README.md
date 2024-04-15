# Data-Science-Project

TO DO: Purpose of this repository is to analyze...

## Short instructions
- TO DO:

## Data
- TO DO: Add instructions for downloading the data that is not in the repository

## Environment management  
All dependencies are listed in `requirements.txt`.

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
<small>(Merge conflicts caused by dependencies can be a bit hard to solve manually.)</small>
1. Double check that you have activated the right virtual environment locally. This is to make sure that you are updating the dependencies from the desired local environment.   
1. Add the new dependencies using `pip` package manager.
1. Run `pip freeze > requirements.txt` in order to update `requirements.txt`.
1. Commit and push the changes to github.
  