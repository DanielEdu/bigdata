# Comandos GIT

First, let’s view the contents of our global Git configuration file.
To do so we pass the --global and the --list options to the git config command.

`git config --global --list`

For our purposes, we are only interested in the user.name, user.email, and init.defaultBranch variables.

`git config --global user.name “<name>”`

`git config --global user.email “<email>”`

Set the default branch name in the global Git configuration.

`git config --global init.defaultBranch “<branch_name”>`

Repository is represented by a hidden directory called .git that exists within a project directory and it contains
all the data on the changes that have been made to the files in a project.

`git init`

El comando git status nos dice el estado de nuestro directorio de trabajo y nuestra área de preparación y la diferencia entre los dos.

`git status`

Add filename to the staging area

`git add <filename>`

Add all the files in the current directory to the staging area

`git add .`

Create a new commit
`git commit -m “<message>”`

Show a list of commits in reverse chronological order
`git log`

## Branches

List the branches
`git branch`

Create a branch
`git branch <new_branch_name>`

Switch branches
`git switch <branch_name>`

Create + switch
`git checkout -b <branch_name>`

Integrate changes from one branch into another branch
`git merge <branch>`

## Clone a Git remote repository

Clonar un repositorio remoto
`git clone <git https | ssh>`

listar repositorios remotos
`git remote -v`

Jalar cambios desde el repositorio remoto
`git pull origin <branch_name>`

Enviar mis cambios al repositorio remoto
`git push origin <branch_name>`
