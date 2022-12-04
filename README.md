# Concurrency Control Protocol Simulation

> Simulasi algoritma <i>concurrency control protocol</i> menggunakan Python3

## Table of Contents

- [How to run](#how-to-run)
- [Usage](#usage)
- [Contact](#contact)

## How to Run

Run the program using <i>command line</i> of your favorite.

Format:
`python3 main.py <file_name> <method>`

File has to be put inside `/test` folder and is a text file.

Method is one of:

- `simple-locking` for Simple Locking (Exclusive lock only)
- `simple-optimistic-cc` for Serial Optimistic Concurrency Control (Validation Based Protocol)
- `multiversion-ts-ord-cc` fir Multiversion Timestamp Ordering Concurrency Control

## Usage

We provided several test cases inside `/test` folder. If you wish to simulate another transaction
scenario, create a new text file inside `/test`. Here's the format of the file

- First line: Number of threads running concurrently
- Second line: Variables involved in transactions
- Third line: Transactions, separated with `;` sign. Here's the detail about the format:
  - `R1(X)` for "Thread 1 read from X"
  - `W1(X)` for "Thread 2 write to X"
  - `A1` for "Abort transactions happening in thread 1"
  - `C1` for "Commit transactions happening in thread 1"

## Contact

Made by:

[@rannnayy](https://github.com/rannnayy) - Maharani Ayu / 13520019

[@Vincent136](https://github.com/Vincent136) - Vincent Christian Siregar / 13520136

As part of Group 4 from Class 1:

[@Nk-Kyle](https://github.com/Nk-Kyle) - Ng Kyle / 13520040

[@fernaldy112](https://github.com/fernaldy112) - Fernaldy / 13520112
