# Shepherd Demo Recording
# Run with: vhs assets/demo.tape

Output assets/demo.gif

Set Shell "bash"
Set FontSize 13
Set Width 1000
Set Height 550
Set Theme "Catppuccin Mocha"
Set Padding 10

# Setup demo data (hidden)
Hide
Type "cd /Users/jiwan/projects/shepherd && python demo.py && export SHEPHERD_STATE_DIR=~/.slurm_shepherd_demo"
Enter
Sleep 1s
Show

# Start the TUI
Type "shepherd"
Enter
Sleep 2.5s

# Navigate through runs
Type "j"
Sleep 600ms
Type "j"
Sleep 600ms
Type "j"
Sleep 600ms
Type "j"
Sleep 600ms

# Go back up
Type "g"
Sleep 800ms

# Switch to script panel
Tab
Sleep 1.5s

# Scroll script
Type "]"
Sleep 300ms
Type "]"
Sleep 300ms
Type "]"
Sleep 800ms

# Switch to logs panel
Tab
Sleep 1.5s

# Back to info
Tab
Sleep 1s

# Navigate to different run
Type "j"
Sleep 500ms
Type "j"
Sleep 1s

# Quit
Type "q"
Sleep 500ms
