#!/usr/bin/env python3
"""
============================================================================
AUTH MODULE - SafeTwin X5 API
============================================================================
Authentification JWT pour l'API FastAPI

Fonctionnalités:
- Login / Register
- JWT Access & Refresh tokens
- Password hashing (bcrypt)
- Protected routes decorator

Version: 1.0.0
Date: 2026-01-14
============================================================================
"""

import os
from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass

from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext
import jwt

# Configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "safetwin-x5-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Security
security = HTTPBearer()

# Router
router = APIRouter(prefix="/auth", tags=["Authentication"])


# ============================================================================
# MODELS
# ============================================================================

class UserBase(BaseModel):
    email: EmailStr
    name: str
    organization: Optional[str] = None

class UserCreate(UserBase):
    password: str

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class User(UserBase):
    id: str
    role: str = "viewer"
    created_at: str
    
    class Config:
        from_attributes = True

class Token(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    user: User

class RefreshTokenRequest(BaseModel):
    refresh_token: str


# ============================================================================
# FAKE DATABASE (remplacer par vraie DB en production)
# ============================================================================

# Stockage en mémoire pour démo
fake_users_db: dict = {}

def get_user_by_email(email: str) -> Optional[dict]:
    return fake_users_db.get(email)

def create_user(user_data: UserCreate) -> dict:
    user_id = f"user_{len(fake_users_db) + 1}"
    user = {
        "id": user_id,
        "email": user_data.email,
        "name": user_data.name,
        "organization": user_data.organization,
        "role": "viewer",
        "hashed_password": pwd_context.hash(user_data.password),
        "created_at": datetime.utcnow().isoformat(),
    }
    fake_users_db[user_data.email] = user
    return user


# ============================================================================
# JWT UTILS
# ============================================================================

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire, "type": "access"})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def create_refresh_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "type": "refresh"})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(token: str, token_type: str = "access") -> dict:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("type") != token_type:
            raise HTTPException(status_code=401, detail="Invalid token type")
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


# ============================================================================
# DEPENDENCIES
# ============================================================================

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """Dependency pour récupérer l'utilisateur courant depuis le token"""
    payload = verify_token(credentials.credentials)
    email = payload.get("sub")
    if not email:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    user = get_user_by_email(email)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    
    return user

async def get_current_admin(user: dict = Depends(get_current_user)) -> dict:
    """Dependency pour vérifier que l'utilisateur est admin"""
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


# ============================================================================
# ENDPOINTS
# ============================================================================

@router.post("/register", response_model=Token)
async def register(user_data: UserCreate):
    """Créer un nouveau compte utilisateur"""
    # Vérifier si email existe déjà
    if get_user_by_email(user_data.email):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    # Créer utilisateur
    user = create_user(user_data)
    
    # Générer tokens
    access_token = create_access_token({"sub": user["email"]})
    refresh_token = create_refresh_token({"sub": user["email"]})
    
    return Token(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        user=User(
            id=user["id"],
            email=user["email"],
            name=user["name"],
            organization=user["organization"],
            role=user["role"],
            created_at=user["created_at"]
        )
    )

@router.post("/login", response_model=Token)
async def login(credentials: UserLogin):
    """Connexion utilisateur"""
    user = get_user_by_email(credentials.email)
    
    if not user or not verify_password(credentials.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )
    
    # Générer tokens
    access_token = create_access_token({"sub": user["email"]})
    refresh_token = create_refresh_token({"sub": user["email"]})
    
    return Token(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        user=User(
            id=user["id"],
            email=user["email"],
            name=user["name"],
            organization=user["organization"],
            role=user["role"],
            created_at=user["created_at"]
        )
    )

@router.post("/refresh", response_model=Token)
async def refresh_token(request: RefreshTokenRequest):
    """Rafraîchir le token d'accès"""
    payload = verify_token(request.refresh_token, token_type="refresh")
    email = payload.get("sub")
    
    user = get_user_by_email(email)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    
    # Générer nouveaux tokens
    access_token = create_access_token({"sub": email})
    new_refresh_token = create_refresh_token({"sub": email})
    
    return Token(
        access_token=access_token,
        refresh_token=new_refresh_token,
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        user=User(
            id=user["id"],
            email=user["email"],
            name=user["name"],
            organization=user["organization"],
            role=user["role"],
            created_at=user["created_at"]
        )
    )

@router.get("/me", response_model=User)
async def get_me(current_user: dict = Depends(get_current_user)):
    """Récupérer le profil de l'utilisateur courant"""
    return User(
        id=current_user["id"],
        email=current_user["email"],
        name=current_user["name"],
        organization=current_user["organization"],
        role=current_user["role"],
        created_at=current_user["created_at"]
    )

@router.post("/logout")
async def logout():
    """Déconnexion (côté client, invalider le token)"""
    # En production, ajouter le token à une blacklist
    return {"message": "Successfully logged out"}


# ============================================================================
# USAGE EXAMPLE
# ============================================================================
"""
Pour utiliser dans main.py:

from api.auth import router as auth_router

app.include_router(auth_router)

# Protéger un endpoint:
from api.auth import get_current_user

@app.get("/protected")
async def protected_route(user: dict = Depends(get_current_user)):
    return {"message": f"Hello {user['name']}"}
"""
