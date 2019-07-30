import unittest

# import your test modules
import testtrecrun, testtreceval

# initialize the test suite
loader = unittest.TestLoader()
suite  = unittest.TestSuite()

# add tests to the test suite
suite.addTests(loader.loadTestsFromModule(testtrecrun))
suite.addTests(loader.loadTestsFromModule(testtreceval))

# initialize a runner, pass it your suite and run it
runner = unittest.TextTestRunner(verbosity=3)
result = runner.run(suite)
