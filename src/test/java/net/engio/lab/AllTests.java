package net.engio.lab;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {
LaboratoryTest.class,
ExecutionContextTest.class})
public class AllTests {
}
