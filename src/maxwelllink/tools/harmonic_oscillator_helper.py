# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Harmonic oscillator helper functions for MD simulations.
"""

import numpy as np
from typing import Optional
from ..units import AU_TO_K, AU_TO_FS

class Dummy_initializer:
    """
    A dummy initializer that returns zero momenta and positions.
    """

    def __init__(self):
        pass
    
    def momentum_initializer(self, p):
        size = p.shape if isinstance(p, np.ndarray) else (len(p),)
        return np.zeros(size)
    
    def position_initializer(self, q):
        size = q.shape if isinstance(q, np.ndarray) else (len(q),)
        return np.zeros(size)
    
class Dummy_thermostat:
    """
    A dummy thermostat that does nothing.
    """

    def __init__(self):
        pass
    
    def apply_kick(self, momentum: np.ndarray) -> np.ndarray:
        return momentum
    
class Maxwell_Boltzmann_initializer:
    """
    A class to initialize particle velocities based on the Maxwell-Boltzmann distribution at the given temperature.
    """

    def __init__(self, temperature_au: float, random_seed: Optional[int] = 114514):
        """
        Parameters
        ----------
        T_au : float
            Temperature in atomic units. k_B is set to 1 in atomic units, so T_au is effectively the thermal energy. Must be positive.
        random_seed : int, optional
            Random seed for reproducible results.
        """
        if temperature_au <= 0:
            raise ValueError("Temperature must be positive.")
        self.temperature_au = temperature_au
        self.random_seed = random_seed
        self.rng = np.random.default_rng(self.random_seed)
    
    def momentum_initializer(self, p):
        if np.all(p == 0):
            print(f"[Maxwell-Boltzmann Initializer] Initializing momenta with Maxwell-Boltzmann distribution at temperature {self.temperature_au} au.")
            size = p.shape if isinstance(p, np.ndarray) else (len(p),)
            p_mb = self.rng.normal(scale=np.sqrt(self.temperature_au), size=size)
            if p_mb.size == 3 :
                return p_mb
            p_mb -= np.mean(p_mb, axis=0)  # remove any net momentum to ensure total momentum is zero
            T_cur_p = np.sum(p_mb**2) / (p_mb.size - 3)
            scaling_factor_p = np.sqrt(self.temperature_au / T_cur_p)
            return p_mb * scaling_factor_p
        else:
            print("[Maxwell-Boltzmann Initializer] Warning: Initial momenta are provided, skipping Maxwell-Boltzmann initialization.")
            return p
    
    def position_initializer(self, omega, q):
        if np.all(q == 0):
            print(f"[Maxwell-Boltzmann Initializer] Initializing positions with Maxwell-Boltzmann distribution at temperature {self.temperature_au} au.")
            size = q.shape if isinstance(q, np.ndarray) else (len(q),)
            q_mb = self.rng.normal(scale=np.sqrt(self.temperature_au) / omega, size=size)
            if q_mb.size == 3 :
                return q_mb
            q_mb -= np.mean(q_mb, axis=0)  # remove any net displacement to ensure total displacement is zero
            T_cur_q = np.sum((omega * q_mb)**2) / (q_mb.size - 3)
            scaling_factor_q = np.sqrt(self.temperature_au / T_cur_q)
            return q_mb * scaling_factor_q
        else:
            print("[Maxwell-Boltzmann Initializer] Warning: Initial positions are provided, skipping Maxwell-Boltzmann initialization.")
            return q
        
class LangevinThermostat:
    """
    Langevin thermostat implementation for the cavity mode.
    """
    def __init__(self, temperature_au: float, dt_au: float, tau_au: float, random_seed: Optional[int] = 114514):
        '''
        Parameters
        ----------
        temperature_au : float
            Temperature in atomic units. k_B is set to 1 in atomic units, so temperature_au is effectively the thermal energy. Must be positive.
        dt_au : float
            Time step in atomic units. Must be positive.
        tau_au : float
            Relaxation time in atomic units. Must be positive. 
        random_seed : int, optional
            Random seed for reproducible results.
        '''
        if temperature_au <= 0:
            raise ValueError("Temperature must be positive.")
        self.temperature_au = temperature_au

        if dt_au <= 0:
            raise ValueError("Time step must be positive.")
        self.dt_au = dt_au

        if tau_au <= 0:
            raise ValueError("Relaxation time must be positive.")
        self.tau_au = tau_au

        self.random_seed = random_seed
        self.rng = np.random.default_rng(self.random_seed)
        self.T_l = np.exp(- self.dt_au / self.tau_au)
        self.S_l = np.sqrt(self.temperature_au * (1.0 - self.T_l**2))
        print(f"[LangevinThermostat] NVT thermostat enabled with T = {self.temperature_au*AU_TO_K} K = {self.temperature_au} a.u., Langevin tau = {self.tau_au*AU_TO_FS} fs = {self.tau_au} a.u.")
        
    def apply_kick(self, momentum: np.ndarray) -> np.ndarray:
        random_kick = self.rng.normal(0, self.S_l, size=momentum.shape)
        return momentum * self.T_l + random_kick