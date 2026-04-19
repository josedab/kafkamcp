package server

func (s *Server) registerTools() {
	s.registerP0Tools()
	s.registerP1Tools()
	s.registerP2Tools()
	s.registerP3Tools()
	s.registerStreamingTools()
	s.registerAdvancedTools()
	s.registerFeatureTools()
}
