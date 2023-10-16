FROM jupyter/minimal-notebook:python-3.8

# Install & use pipenv
COPY Pipfile Pipfile.lock setup.py test_environment.py .env ./ 

RUN pip install pipenv
RUN pipenv install --system