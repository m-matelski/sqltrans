from sqltrans.utils import ChangingListIterator


class TestChangingListIterator:
    def test_on_empty_list(self):
        lst = []
        result = [i for i in ChangingListIterator(lst)]
        assert result == []

    def test_iteration_without_list_modification(self):
        lst = list(range(10))
        result = [i for i in ChangingListIterator(lst)]
        assert result == lst

    def test_iteration_with_element_deletion(self):
        lst = list(range(10))
        expected = list(lst)
        new_list = []
        for idx, i in enumerate(ChangingListIterator(lst)):
            new_list.append(i)
            if idx % 2 == 0:
                del lst[lst.index(i)]
        assert expected == new_list
        assert lst == [1, 3, 5, 7, 9]

    def test_iteration_with_future_element_deletion(self):
        lst = list(range(10))
        new_list = []
        deleted = False
        for idx, i in enumerate(ChangingListIterator(lst)):
            if not deleted:
                del lst[7]
                deleted = True
            new_list.append(i)
        assert lst == new_list

    def test_iteration_with_element_addition(self):
        lst = list(range(10))
        new_list = []
        added = False
        for idx, i in enumerate(ChangingListIterator(lst)):
            if not added:
                lst[4:5] = [11, 22, 33]
                added = True
            new_list.append(i)
        assert [0, 1, 2, 3, 11, 22, 33, 5, 6, 7, 8, 9] == new_list

    def test_iteration_with_enumerate(self):
        """Test that enumerate iteration allows to use generated index to modify a list"""
        lst = list(range(10))
        expected = list(lst)
        new_list = []
        for idx, i in ChangingListIterator(lst).enumerate():
            new_list.append(i)
            if i % 2 == 0:
                del lst[idx]
        assert expected == new_list
        assert lst == [1, 3, 5, 7, 9]
