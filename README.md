# Dolt Fuzzer

<img align="left" height="225" src="https://user-images.githubusercontent.com/5618869/114187874-567b4880-98fd-11eb-8cf5-2a0501a8fb88.gif"/>

This tool has the core ability to randomly generate a collection of Dolt repositories, and to perform some kind of action(s) on them. This is for the purpose of fuzzing Dolt. All generated repositories will be created based on the parameters from the configuration file.

Commands are a way to interact with the generated repositories in some way, or to influence the generation itself. If no command is given to the fuzzer, then a repository is generated as mentioned earlier. However, commands may hook into the generation phase or provide a test routine. For more information on the commands, please view the [README in the relevant folder](./commands/README.md).

## Configuration

### General Configurable Options

* Invalid Name Regexes
    * Branches
    * Tables
    * Columns
    * Indexes
    * Constraints
* Amounts
    * Branches
    * Tables
    * Primary Keys
    * Columns
    * Indexes
    * Foreign Key Constraints
    * Rows
    * Index Delay
* Statement Distribution
    * INSERT
    * REPLACE
    * UPDATE
    * DELETE
* Interface Distribution
    * CLI Query
    * CLI Batch
    * SQL Server
    * Consecutive Range
* Options
    * Dolt Version/Hash
    * Auto GC
    * Manual GC
    * Include README Config
    * Enforce Rows Lower Bound on Master Only
    * Logging
    * Port
* Type Parameters
    * Applicable Types
* Type Distribution
    * All Types

### Section Breakdown

* Invalid Name Regexes
    * Provides a regex that generated names are matched against. If the name fails the regex, then a new one is generated until a passing name is found. If we try 10,000,000 times and still fail, then we abort the cycle.
* Amounts
    * Specifies the range for that value in the format `[x, y]`, where `x` is the lower bound and `y` is the upper bound (both inclusive). For example, `Rows = [10, 1000]` means that all generated repositories will contain tables with at least 10 rows but no more than 1000.
* Statement Distribution
    * Specifies the rough distribution of the SQL operations. The percentage frequency is determined by the statement's number divided by the sum of all statement' numbers. If a range is given rather than a number, then each cycle will choose a number from the range. A value of 0 will prevent a statement from occurring.
    * It is recommended to set DELETE to a value less than the sum of INSERT and REPLACE, otherwise you may dramatically increase cycle run times.
* Interface Distribution
    * Specifies the rough distribution of the interface to use for a statement. The percentage frequency is determined by the interface's number divided by the sum of all interfaces' numbers. If a range is given rather than a number, then each cycle will choose a number from the range. A value of 0 will prevent an interface from being used.
    * Consecutive range allows for multiple statements to be sent over an interface. For the server, this will shorten cycle run time. The overall distribution is kept intact, as the larger the consecutive range for an interface, the lower its distribution number until it normalizes.
    * CLI_Query may be set to 0 by the cycle if it is determined that queries would be too long to be passed as an argument.
* Options
    * These are options that apply to all cycles for this run.
    * Auto GC is whether auto GC is enabled. Manual GC will run gc in rough intervals. 
* Type Parameters
    * Controls the parameter ranges for the listed parameters. All parameter ranges must be valid for the relevant type. For example, setting the length of a `VARCHAR` to zero is illegal, and will throw an error.
* Type Distribution
    * Determines the frequency that the type will occur, given as either a number or a range in the format `[x, y]`. A value of 0 will prevent the type from being used.

