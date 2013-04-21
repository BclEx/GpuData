namespace Core
{
	class MutexEx 
	{
		inline static void Enter(MutexEx mutex) { }
		inline static void Leave(MutexEx mutex) { }
		inline static bool Held(MutexEx mutex) { return true; }
		inline static bool NotHeld(MutexEx mutex) { return true; }
		inline static void Free(MutexEx mutex) { }
	};
}
