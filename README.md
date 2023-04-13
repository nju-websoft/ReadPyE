# ReadPyE
Revisiting Knowledge-Based Inference of Python Runtime Environments: A Realistic and Adaptive Approach

## Dependencies

This project is developed using Python 3.8.11 on Ubuntu 18.04 LTS.
Install the required Python packages:

```
pip install -r requirements.txt
```

[Tree-sitter setup](https://github.com/tree-sitter/py-tree-sitter#setup):
```
mkdir $LANGUAGE_DIR
cd $LANGUAGE_DIR; mkdir build vendor
cd vendor; git clone https://github.com/tree-sitter/tree-sitter-python
```

Install Docker and Neo4j (requires Java SE 11):

| Name    | Version  |
| ------- | -------- |
| Docker  | 20.10.22 |
| Neo4j   | 4.3.10   |

We have openned our pre-built **knowledge graphs** (for package sets S<sub>0</sub> and S<sub>1</sub>), self-built **IntegrGitHub dataset**, and **experimental results** as two online resources: [resource 0](https://figshare.com/articles/online_resource/Revisiting_Knowledge-Based_Inference_of_Python_Runtime_Environments_A_Realistic_and_Adaptive_Approach/22590364) and [resource 1](https://figshare.com/articles/online_resource/Revisiting_Knowledge-Based_Inference_of_Python_Runtime_Environments_A_Realistic_and_Adaptive_Approach/22597234). 

Restore one database dump:

```
$NEO4J_HOME/bin/neo4j-admin load --from=xxx.dump --database=neo4j --force
```

APOC Core is required: moving the APOC jar file from the `$NEO4J_HOME/labs` directory to the `$NEO4J_HOME/plugins` directory and restarting Neo4j:

```
$NEO4J_HOME/bin/neo4j start
```

Config neo4j database by editing `utils/variables.py`:

```
NEO4J_URI = 'YOUR NEO4J URI'
NEO4J_USER = 'YOUR NEO4J USERNAME'
NEO4J_PWD = 'YOUR NEO4J PASSWORD'
```

## Usage

The API of ReadPyE is `AutomaticInference.main` in `run.py`.

You can use ReadPyE in commands. See the helps by

```
python run.py -h
```

*Note that one-time use via the command line is inefficient, as some resources are required to be loaded.

### Example
Use ReadPyE without iterative validation:

```
python run.py -l $LANGUAGE_DIR -p examples/3979513/snippet.py
```

The inferred Dockerfile is:

```
FROM python:2.7.18
RUN pip install --upgrade pip
RUN pip install twisted==20.3
RUN pip install zope-interface==5.5.dev0
```

The validation result in environment-related exception:
```
Traceback (most recent call last):
File "snippet.py", line 12, in <module>
    from twisted.conch import error, avatar
File "/usr/local/lib/python2.7/site-packages/twisted/conch/avatar.py", line 9, in <module>
    from twisted.conch.ssh.connection import OPEN_UNKNOWN_CHANNEL_TYPE
File "/usr/local/lib/python2.7/site-packages/twisted/conch/ssh/connection.py", line 17, in <module>
    from twisted.conch.ssh import service, common
File "/usr/local/lib/python2.7/site-packages/twisted/conch/ssh/common.py", line 15, in <module>
    from cryptography.utils import int_from_bytes, int_to_bytes
ImportError: No module named cryptography.utils
```

Use complete ReadPyE:

```
python run.py -l $LANGUAGE_DIR -p examples/3979513/snippet.py -s examples/3979513/validation_settings.json
```

The inferred Dockerfile is: &#x2705;

```
FROM python:2.7.18
RUN pip install --upgrade pip
RUN pip install twisted==20.3
RUN pip install cryptography==3.3.2
RUN pip install bcrypt==3.1.7
RUN pip install pyasn1==0.5rc1
RUN pip install mysqlclient==1.4.6
RUN pip install zope-interface==5.5.dev0
```