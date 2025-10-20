from typing import Union
import random
from collections.abc import KeysView

"""
Small utils to help with lists
"""


def remove_duplicates(l1: Union[list, KeysView]):
    """
    Gives a list without duplicates.
    """
    return list(set(l1))


def intersection(l1: Union[list, KeysView], l2: Union[list, KeysView]):
    """
    Gives a list containing only the elements in common
    between the two input lists.
    """
    return list(set(l1).intersection(l2))


def difference(l1: Union[list, KeysView], l2: Union[list, KeysView]):
    """
    Gives a list containing the elements of l1
    not in l2.
    """
    return list(set(l1).difference(l2))


def union(l1: Union[list, KeysView], l2: Union[list, KeysView]):
    """
    Gives a list containing the elements of l1
    and l2, without overlap.
    """
    return list(set(l1).union(l2))


def is_sublist(l1: Union[list, KeysView], l2: Union[list, KeysView]):
    """
    Returns a bool whether all elements of l1
    are contained in l2.
    An empty list is always a sublist of another
    empty or non-empty list.
    """
    if len(l1) == 0 or len(l2) == 0:
        return True
    else:
        return len(intersection(l1, l2)) == len(list(set(l1)))


def has_same_elements(l1: Union[list, KeysView], l2: Union[list, KeysView]):
    """
    Returns a bool whether all elements in l1
    are contained in l2 and nothing more.
    If both lists are empty, True is returned.
    """
    if len(l1) == 0:
        if len(l2) == 0:
            return True
        else:
            return False
    elif len(l2) == 0:
        if len(l1) == 0:
            return True
        else:
            return False
    else:
        return is_sublist(l1, l2) and is_sublist(l2, l1)


def list_to_dict(l: Union[list, KeysView]):
    """
    Encodes a list as a dictionary with the keys being
    0, ..., len(l), and the value associated with each
    key being l[i].
    """
    return {k: v for (k, v) in zip(range(len(l)), l)}


def is_element_no_case(x, l: Union[list, KeysView]):
    """
    Returns a bool whether x is in l, ignoring the case if it is a string.
    If the list is empty, True is returned.
    """
    assert isinstance(l, (list, KeysView)), "l must be a list or a KeysView"
    if len(l) == 0:
        return True
    else:
        if isinstance(x, str):
            for e in l:
                if isinstance(e, str):
                    if x.lower() == e.lower():
                        return True
                    else:
                        continue
                else:
                    continue
            return False
        else:
            if x in l:
                return True
            else:
                return False


def order_list_by_reference(l1: list, l2: list):
    """
    Orders list l2 based on the order of elements in list l1.

    Elements in l2 that are also in l1 will be ordered according to their
    appearance in l1. Elements in l2 that are not in l1 will be placed
    at the end of the resulting list in their original relative order.

    Args:
        l1: Reference list that defines the ordering
        l2: List to be reordered

    Returns:
        A new list with elements from l2 ordered according to l1
    """
    # Create a set for O(1) lookup of elements in l1
    l1_set = set(l1)

    # Separate elements in l2 that are in l1 vs not in l1
    in_l1 = []
    not_in_l1 = []

    for item in l2:
        if item in l1_set:
            in_l1.append(item)
        else:
            not_in_l1.append(item)

    # Order the elements that are in l1 according to l1's order
    # Create a mapping of element to its index in l1 for sorting
    l1_index = {item: idx for idx, item in enumerate(l1)}

    # Sort elements that are in l1 based on their order in l1
    ordered_in_l1 = sorted(in_l1, key=lambda x: l1_index[x])

    # Combine ordered elements with elements not in l1
    return ordered_in_l1 + not_in_l1


def sample_elements(l: list, n: Union[int, None], seed=None):
    if seed is not None:
        random.seed(seed)
    if n is None:
        return l
    else:
        return random.sample(l, k=min(n, len(l)))


if __name__ == "__main__2":
    a = [1, 2, 3, 4]
    b = [2, 4, 6]
    c = [1, 2, 3]
    d = [4, 1, 2, 3]

    intersection(a, b)
    difference(a, b)
    union(a, b)
    is_sublist(c, a)
    has_same_elements(a, d)
    list_to_dict(a)

    is_element_no_case("b", ["a", 1, "Non", "Nonee", None])

    # Test case 1: Basic ordering
    l1 = ["a", "b", "c", "d"]
    l2 = ["d", "b", "a", "e", "c", "f"]
    result = order_list_by_reference(l1, l2)
    print(f"l1: {l1}")
    print(f"l2: {l2}")
    print(f"result: {result}")
    # Expected: ['a', 'b', 'c', 'd', 'e', 'f']

    print()

    # Test case 2: Numbers
    l1 = [1, 3, 5, 7, 9]
    l2 = [9, 2, 5, 8, 1, 4, 3]
    result = order_list_by_reference(l1, l2)
    print(f"l1: {l1}")
    print(f"l2: {l2}")
    print(f"result: {result}")
    # Expected: [1, 3, 5, 9, 2, 8, 4]

    print()

    # Test case 3: l2 has duplicates
    l1 = ["x", "y", "z"]
    l2 = ["z", "x", "a", "y", "x", "b"]
    result = order_list_by_reference(l1, l2)
    print(f"l1: {l1}")
    print(f"l2: {l2}")
    print(f"result: {result}")
    # Expected: ['x', 'x', 'y', 'z', 'a', 'b']

    print()

    # Test case 4: Empty lists
    l1 = []
    l2 = ["a", "b", "c"]
    result = order_list_by_reference(l1, l2)
    print(f"l1: {l1}")
    print(f"l2: {l2}")
    print(f"result: {result}")
    # Expected: ['a', 'b', 'c'] (all elements not in l1, so at the end)
