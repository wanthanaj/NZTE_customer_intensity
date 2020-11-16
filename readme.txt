


-- To set up the environment, installing all required package, run the command below:
pip install -r requirements.txt



-- list all environment and check what environment is activated (should have * in the front)
conda env list
-- create new environment
conda create --name project-env
-- activate the new environment (if this doesn't work, run VScode from the command prompt - typing "code")
conda activate [environment name]
-- check the new environment
conda list


source:
https://towardsdatascience.com/manage-your-python-virtual-environment-with-conda-a0d2934d5195
https://protostar.space/why-you-need-python-environments-and-how-to-manage-them-with-conda#:~:text=Another%20reason%20is%20that%20applying,(s)%20on%20both%20computers.
https://jonathanchu.is/posts/virtualenv-and-pip-basics/


freeze                      Output installed packages in requirements format.
list                        List installed packages

--export package requirements
pip freeze > requirements.txt
--command for next time set up new environment
pip install -r requirements.txt