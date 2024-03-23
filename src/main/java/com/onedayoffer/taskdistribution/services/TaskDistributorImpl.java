package com.onedayoffer.taskdistribution.services;

import com.onedayoffer.taskdistribution.DTO.*;
import jakarta.transaction.Transactional;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.crypto.spec.PSource;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TaskDistributorImpl implements TaskDistributor {

    @Override
    @Transactional
    public void distribute(List<EmployeeDTO> employees, List<TaskDTO> tasks) {
        validate(employees, tasks);

        log.debug("Старт дистрибьюции. Количество задач: {}. Количество специалистов {}", tasks.size(), employees.size());
        List<TaskDTO> sortedTask = tasks.stream()
                .filter(it -> it.getLeadTime() != null)
                .sorted(Comparator.comparing(TaskDTO::getPriority))
                .toList();

        List<TaskDTO> notDistributedTask = new ArrayList<>();
        for (TaskDTO task : sortedTask) {
            boolean distributeTask = false;
            for (EmployeeDTO employee : employees) {
                if (employee.containsTaskWithPriority(task.getPriority()) || distributeTask) {
                    continue;
                }
                if (!isAllowedAddTask(task, employee, 420)) {
                    continue;
                }
                employee.getTasks().add(task);
                distributeTask = true;
            }
            if (!distributeTask) {
                notDistributedTask.add(task);
            }
        }

        if (!notDistributedTask.isEmpty()) {
            simpleDistribute(employees, notDistributedTask);
        }

        long distributeTaskCount = employees.stream()
                .mapToLong(it -> (long)it.getTasks().size())
                .sum();
        if (distributeTaskCount < (long)tasks.size()) {
            log.warn("Внимание! остались нераспределенные задачи");
        }

        log.debug("Завершена дистрибьюция. Количество задач: {}. Количество специалистов {}", tasks.size(), employees.size());
    }

    private static void validate(List<EmployeeDTO> employees, List<TaskDTO> tasks) {
        if (employees.isEmpty()) {
            throw new ValidationException("Дистрибьюция не выполенена, т.к. не выбраны специалист");
        }
        if (tasks.isEmpty()) {
            throw new ValidationException("Дистрибьюция не выполенена, т.к. не выбраны задачи");
        }
        boolean anyLeadTimeIsNull = tasks.stream().anyMatch(it -> it.getLeadTime() == null);
        if (anyLeadTimeIsNull) {
            log.warn("Есть задачи с незаданным временем!");
        }
        int sumTaskLeadTime = tasks.stream()
                .filter(it -> it.getLeadTime() != null)
                .mapToInt(TaskDTO::getLeadTime)
                .sum();
        if (sumTaskLeadTime > employees.size() * 420) {
            log.warn("Суммарное время сотрудников меньше необходимого времени для выполнения задач. Не все задачи будут распределены");
        }
        if (sumTaskLeadTime < employees.size() * 360) {
            log.warn("Не все сотрудники будут загружены на 6 часов, т.к. не хватает задач");
        }
    }

    private static boolean isAllowedAddTask(TaskDTO task, EmployeeDTO employee, Integer time) {
        int totalLeadTime = employee.getTotalLeadTime();
        return totalLeadTime < time && totalLeadTime + task.getLeadTime() < time;
    }

    private static void simpleDistribute(List<EmployeeDTO> employees, List<TaskDTO> notDistributedTask) {
        employees = employees.stream().sorted(Comparator.comparing(it->it.getTotalLeadTime()))
                .collect(Collectors.toList());
        for (TaskDTO task : notDistributedTask) {
            boolean distributeTask = false;
            for (EmployeeDTO employee : employees) {
                if (distributeTask) {
                    continue;
                }
                if (!isAllowedAddTask(task, employee, 420)) {
                    continue;
                }
                employee.getTasks().add(task);
                distributeTask = true;
            }
        }
    }

}
