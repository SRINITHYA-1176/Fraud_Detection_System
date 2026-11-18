package com.example.fraud_detection_system.controller;

import com.example.fraud_detection_system.service.DashboardService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class DashboardController {
  private final DashboardService dashboardService;

  // NEW: Inject DashboardService
  public DashboardController(DashboardService dashboardService) {
    this.dashboardService = dashboardService;
  }

  // NEW: Open the Thymeleaf dashboard page
  @GetMapping("/dashboard")
  public String dashboard(Model model) {
    // NEW: Fetch statistics from PostgreSQL
    model.addAttribute("stats", dashboardService.getDashboardStatistics());
    // NEW: Fetch recent transactions and send them to the dashboard.
    model.addAttribute("recentTransactions", dashboardService.getRecentTransactions());
    return "dashboard";
  }
}
