# Contributing Guide

Thank you for your interest in contributing to this project! Follow the guidelines below to ensure a
smooth collaboration.

---

## Branching Workflow

This project uses a **branching model** to keep the codebase organized and stable:

- **`main`**: This branch contains the stable, production-ready code. Do not directly commit to `main`.
- **`dev_pgs`**: The main development branch. All new features and bug fixes should branch off `dev_pgs`
  and be merged back into `dev_pgs` after testing.
- **Feature Branches**: Short-lived branches for specific tasks. Use the naming convention
  `feature/your-feature-name`.

### Workflow for Contributions

1. **Branch Off `dev_pgs`**:

   - Always create a new branch for your work, branching off `dev_pgs`:

     ```bash
     git checkout dev_pgs
     git checkout -b feature/your-feature-name
     ```

2. **Make Your Changes**:

   - Commit your changes with descriptive commit messages:

     ```bash
     git add .
     git commit -m "Add: Implement feature for X"
     ```

3. **Push Your Branch**:

   - Push your branch to GitHub:

     ```bash
     git push -u origin feature/your-feature-name
     ```

4. **Open a Pull Request (PR)**:

   - Go to GitHub and open a PR to merge your feature branch into `dev_pgs`.
   - Provide a clear description of what your changes do.

5. **Code Review**:

   - Wait for the maintainers or other collaborators to review your PR.
   - Make any requested changes and update your PR.

6. **Merge**:

   - Once approved, your changes will be merged into `dev_pgs`.

---

## Development Tips

### Syncing Your Branch

Always ensure your branch is up to date with the latest changes from `dev_pgs`:

```bash
git checkout dev_pgs
git pull origin dev_pgs
git checkout feature/your-feature-name
git rebase dev_pgs
```

## Running Tests

Run all tests before committing your changes to ensure stability. For example:

```bash
R CMD check .
```

## Undo Mistakes

If you make a mistake, don’t worry! Here are some common fixes:

Undo the last commit but keep your changes:

```bash
git reset --soft HEAD~1
```

Discard all local changes (be careful!):

```bash
git reset --hard
```

