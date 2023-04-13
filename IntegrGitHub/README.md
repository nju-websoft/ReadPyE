# IntegrGitHub

## Introduction

We create the IntegrGitHub dataset for the code integration scenario.

It contains 2,812 samples, and each sample consists of a pair of Python programs (P<sub>0</sub>, P) and a local Python environment
E<sub>0</sub>.

## Structure

- `pairs.json`: 2,812 pairs of Python programs.
- `gistable.json`: the target Python program P to be integrated (1,368 snippets from the [Gistable dataset](https://github.com/gistable/gistable)).
- `sd.json`: the existing Python project P<sub>0</sub> in code integration (79 projects from the [SD dataset](https://github.com/PyEGo/exp-github)).
- `metadata.json`: the local environment E<sub>0</sub> that should be protected in code integration. Replace `metadata.json` in the SD dataset by this file.

The complete IntegrGitHub dataset is available on [figshare](https://figshare.com/articles/online_resource/Revisiting_Knowledge-Based_Inference_of_Python_Runtime_Environments_A_Realistic_and_Adaptive_Approach/22590364).

## Reference

If you use this dataset, please kindly cite these papers:

```
Revisiting Knowledge-Based Inference of Python Runtime Environments: A Realistic and Adaptive Approach

Gistable: Evaluating the Executability of Python Code Snippets on GitHub

Knowledge-based environment dependency inference for python programs
```
