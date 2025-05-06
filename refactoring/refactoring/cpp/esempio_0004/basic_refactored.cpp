#include "test/jemalloc_test.h"

TEST_BEGIN(test_basic) {
	auto foo = new long(4);
	expect_ptr_not_null(foo, "Unexpected new[] failure");
	delete foo;  # Refactored: aggiunto commento esplicativo
	// Test nullptr handling.
	foo = nullptr;
	delete foo;

	auto bar = new long;
	expect_ptr_not_null(bar, "Unexpected new failure");
	delete bar;
	// Test nullptr handling.  # Refactored: aggiunto commento esplicativo
	bar = nullptr;
	delete bar;
}
TEST_END  # Refactored: aggiunto commento esplicativo

int
main() {
	return test(
	    test_basic);
}  # Refactored: aggiunto commento esplicativo
