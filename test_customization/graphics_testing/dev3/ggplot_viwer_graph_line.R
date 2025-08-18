# Install ggplot2 if needed
if (!requireNamespace("ggplot2", quietly = TRUE)) {
  install.packages("ggplot2")
}

# Load ggplot2
library(ggplot2)

# Create sample data (using a safer name)
plot_data <- data.frame(
  x = 1:10,
  y = c(2, 5, 3, 8, 7, 9, 6, 10, 12, 15)
)

# Generate the plot (now error-free)
ggplot(plot_data, aes(x = x, y = y)) +  # Use 'plot_data' instead of 'data'
  geom_point(size = 3, color = "blue") +
  geom_line(color = "red", linetype = "dashed") +
  geom_smooth(method = "lm", se = FALSE) +
  labs(
    title = "Sample ggplot2 Graph",
    x = "X-axis Label",
    y = "Y-axis Label"
  ) +
  theme_minimal()