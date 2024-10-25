import threading
import time

# Глобальные списки для хранения результатов
results_formula_1 = []
results_formula_2 = []


# Определение формул
def formula_1(x):
    return x ** 2 - x ** 2 + x * 4 - x * 5 + x + x


def formula_2(x):
    return x + x


# Функция для вычисления результатов по формуле 1
def compute_formula_1(iterations):
    for x in range(iterations):
        results_formula_1.append(formula_1(x))


# Функция для вычисления результатов по формуле 2
def compute_formula_2(iterations):
    for x in range(iterations):
        results_formula_2.append(formula_2(x))


# Функция для параллельного выполнения вычислений и получения результатов
def compute_formulas(iterations):
    global results_formula_1, results_formula_2

    # Очистка списков результатов перед новыми вычислениями
    results_formula_1.clear()
    results_formula_2.clear()

    # Создаем потоки для параллельного выполнения
    thread_1 = threading.Thread(target=compute_formula_1, args=(iterations,))
    thread_2 = threading.Thread(target=compute_formula_2, args=(iterations,))

    # Запускаем потоки
    start_time = time.time()  # Начинаем отсчет времени
    thread_1.start()
    thread_2.start()

    # Ожидаем завершения потоков
    thread_1.join()
    thread_2.join()

    duration = time.time() - start_time  # Вычисляем длительность выполнения

    # Вычисляем результат по формуле 3
    result_formula_3 = sum(results_formula_1) + sum(results_formula_2)

    return duration, result_formula_3


# Основная функция для запуска программы
def main():
    for iterations in [10000, 100000]:
        duration, result_formula_3 = compute_formulas(iterations)

        # Вывод результатов
        print(f"Итерации: {iterations}")
        print(f"Время выполнения для формул 1 и 2: {duration:.4f} секунд")
        print(f"Результат по формуле 3: {result_formula_3}\n")


# Запуск основной функции
if __name__ == "__main__":
    main()
