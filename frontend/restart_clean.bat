@echo off
echo Cleaning Next.js cache and rebuilding...
rmdir /s /q .next 2>nul
rmdir /s /q node_modules\.cache 2>nul
echo Cache cleared. Starting dev server...
npm run dev

